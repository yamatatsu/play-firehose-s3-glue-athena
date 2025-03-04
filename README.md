# firehoseを使ってs3にparquet形式でデータを保存する

firehoseを使ってs3にparquet形式でデータを保存してみました。
IoT Core * firehose * S3 はよく使うのですが、parquet形式はやったことなかったので勉強になりました。

## 今回やること

- IoT Core -> Firehose -> S3の構成でデータをS3に保存する
  - データはparquet形式で保存する
  - データはSNAPPYで圧縮する
- データはathenaからクエリできるようにする
  - パーティションを用いてコストとパフォーマンスを最適化する
  - パーティションのキーは日付（JST）を用いる
- CDKで構築する
  - cdkデプロイするだけでテストデータの投入も行われる

## 構成図

```mermaid
flowchart TB    
    %% Topic Rule Actions
    IoTCore -- "Rule Action"  --> FirehoseStream["Firehose"]
    
    %% Firehose Processing
    FirehoseStream --> LambdaProcessor["Lambda Function"]
    LambdaProcessor -- Enrich --> FirehoseStream
    
    %% Firehose uses Glue Schema
    FirehoseStream -- "Use Schema" --> GlueTable["Glue Table"]
    
    %% S3 Storage
    FirehoseStream -- "Store Data" --> S3Bucket
    GlueTable -- "Reference" --> S3Bucket

    %% Athena Query (implied)
    Athena["Athena"] -- "Query" --> GlueTable
    
    %% Styling
    classDef aws fill:#FF9900,stroke:#232F3E,color:#232F3E
    classDef storage fill:#3F8624,stroke:#232F3E,color:white
    classDef compute fill:#D86613,stroke:#232F3E,color:white
    classDef analytics fill:#3B48CC,stroke:#232F3E,color:white
    classDef database fill:#2E73B8,stroke:#232F3E,color:white
    classDef integration fill:#CC2264,stroke:#232F3E,color:white
    
    class IoTCore,TopicRule aws
    class S3Bucket storage
    class LambdaProcessor,DataPublisher compute
    class FirehoseStream,Athena analytics
    class GlueDatabase,GlueTable database
    class IoTDevice,ErrorLog integration
    class FirehoseSchemaRole aws
```

## CDK

```ts
// 全文
```

## まとめ

Glue TableはAthenaのためにスキーマ定義を書くときに使っていて「なぜAthenaの機能じゃないんだ？」と思っていました。
今回parquet変換のためにスキーマが使われるのを見て「Athenaのサブ機能ではなかったのか。。。」ということを理解できました。

コードは以下のリポジトリにおいてあります。

https://github.com/yamatatsu/play-firehose-s3-glue-athena