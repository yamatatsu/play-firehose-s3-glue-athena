import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class PlayFirehoseS3GlueAthenaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'PlayFirehoseS3GlueAthenaQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });
  }
}
