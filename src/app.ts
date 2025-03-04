import * as iot from '@aws-cdk/aws-iot-alpha';
import * as iotActions from '@aws-cdk/aws-iot-actions-alpha';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';


const app = new cdk.App();
const stack = new cdk.Stack(app, 'PlayFirehoseS3GlueAthenaStack');

// ================================================
// MQTTで送信されたデータを溜めておくS3バケット
// firehose&glueがparquetに変換してここに置く

const bucket = new s3.Bucket(stack, "Bucket", {
  encryption: s3.BucketEncryption.S3_MANAGED,
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
  // サンプル実装なので
  removalPolicy: cdk.RemovalPolicy.DESTROY,
  autoDeleteObjects: true,
});

// ================================================
// Glueの設定
// 以下の二つの目的のためにGlueを使用する
// - FirehoseがデータをS3に保存する際のparquetスキーマを提供
// - Athenaでデータをクエリするためのスキーマを提供
// 
// firehoseが参照するのはpartitionKeysやcolumnsなどの列定義のみ
// 他の設定はathenaから参照される

const glueDatabase = new glue.Database(stack, "Database");
const glueTable = new glue.S3Table(stack, "S3Table", {
  database: glueDatabase,
  partitionKeys: [
    // glue tableでは、カラム名は全て小文字で扱う必要がある
    { name: "devicename", type: glue.Schema.STRING },
    { name: "date", type: glue.Schema.STRING },
  ],
  columns: [
    { name: "hour", type: glue.Schema.STRING },
    { name: "minute", type: glue.Schema.STRING },
    { name: "second", type: glue.Schema.STRING },
    { name: "metrics", type: glue.Schema.map(glue.Schema.STRING, glue.Schema.FLOAT) },
  ],
  bucket,
  s3Prefix: "data/",
  dataFormat: glue.DataFormat.PARQUET,
  storageParameters: [
    glue.StorageParameter.compressionType(glue.CompressionType.SNAPPY),
  ],
  // partition projectionの設定
  parameters: {
    "projection.enabled": "true",
    "projection.devicename.type": "injected",
    "projection.date.type": "date",
    "projection.date.range": "2025/03/01,NOW",
    "projection.date.format": "yyyy/MM/dd",
    "projection.date.interval": "1",
    "projection.date.interval.unit": "DAYS",
    "storage.location.template": "s3://" + bucket.bucketName + "/data/${devicename}/${date}/",
  },
});

/**
 * JSTタイムゾーンでパーティショニングを行うためにLambda Processorを使用する。
 */
const handler = new NodejsFunction(stack, "FirehoseEnricher", {
  runtime: lambda.Runtime.NODEJS_22_X,
  architecture: lambda.Architecture.ARM_64,
  // firehose上のコンソールで1分以上の値にするように警告された
  timeout: cdk.Duration.minutes(1),
  environment: {
    // 日付を扱うため、重要
    TZ: "Asia/Tokyo",
  },
});

/**
 * firehoseがGlueのSchemaを参照するためのIAMロール
 * 本当はDeliveryStreamから取得したかったけど、公開されてなかった。。。なぜ。。。。
 */
const firehoseSchemaRole = new iam.Role(stack, "FirehoseSchemaRole", {
  assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com"),
  inlinePolicies: {
    catalogPolicy: new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          actions: ["glue:GetTableVersions"],
          resources: [stack.formatArn({ service: "glue", resource: "*" })],
        }),
      ],
    }),
  },
});
glueTable.grantRead(firehoseSchemaRole);

// ================================================
// firehoseの設定

const firehoseStream = new firehose.DeliveryStream(stack, "FirehoseStream", {
  destination: new firehose.S3Bucket(bucket, {
    // バッファは最大に
    bufferingInterval: cdk.Duration.minutes(15),
    bufferingSize: cdk.Size.mebibytes(128),
    // `OutputFormatConfiguration.Serializer.ParquetSerDe.Compression`に指定がある場合、`compression`は指定不要
    compression: firehose.Compression.of("UNCOMPRESSED"),
    processor: new firehose.LambdaFunctionProcessor(handler),
    dataOutputPrefix: "data/!{partitionKeyFromLambda:deviceName}/!{partitionKeyFromLambda:date}/",
    errorOutputPrefix: "error/",
  }),
});
const cfnFirehoseStream = firehoseStream.node.defaultChild as firehose.CfnDeliveryStream;
cfnFirehoseStream.addPropertyOverride(
  "ExtendedS3DestinationConfiguration.DynamicPartitioningConfiguration",
  { Enabled: true },
);
cfnFirehoseStream.addPropertyOverride(
  "ExtendedS3DestinationConfiguration.DataFormatConversionConfiguration",
  /**
   * Glue TableのSchemaを参照してparquetに変換する設定
   */
  {
    Enabled: true,
    /** Glue Tableへの参照 */
    SchemaConfiguration: {
      CatalogId: glueDatabase.catalogId,
      RoleARN: firehoseSchemaRole.roleArn,
      DatabaseName: glueDatabase.databaseName,
      TableName: glueTable.tableName,
      Region: stack.region,
      VersionId: "LATEST",
    },
    /**
     * 入力設定
     * glueでは列名に大文字を含めることができないため、ここで小文字に変換する
     */
    InputFormatConfiguration: {
      Deserializer: {
        OpenXJsonSerDe: { CaseInsensitive: false },
      },
    },
    /**
     * 出力設定
     * SNAPPYで圧縮したparquetを出力する
     */
    OutputFormatConfiguration: {
      Serializer: {
        ParquetSerDe: { Compression: "SNAPPY" },
      },
    },
  },
);

// ================================================
// IoT Coreの設定

const errorLog = new logs.LogGroup(stack, "ErrorLog", {
  retention: logs.RetentionDays.ONE_DAY,
});

new iot.TopicRule(stack, "TopicRule", {
  sql: iot.IotSql.fromStringAsVer20160323("SELECT * FROM 'iot-data'"),
  actions: [
    new iotActions.FirehosePutRecordAction(firehoseStream)
  ],
  errorAction: new iotActions.CloudWatchLogsAction(errorLog),
});

// ================================================
// 擬似的にIoTデータを投入する

const dataPublisher = new NodejsFunction(stack, "DataPublisher", {
  runtime: lambda.Runtime.NODEJS_22_X,
  architecture: lambda.Architecture.ARM_64,
  timeout: cdk.Duration.minutes(15),
  initialPolicy: [
    new iam.PolicyStatement({
      actions: ["iot:Publish"],
      resources: ["*"],
    }),
  ]
});

for (const num of [...Array(3).keys()]) {
  new cdk.triggers.Trigger(stack, `Trigger${num}`, {
    timeout: cdk.Duration.minutes(15),
    handler: dataPublisher,
  })
}
