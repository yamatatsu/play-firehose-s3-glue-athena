import type {
  FirehoseTransformationEvent,
  FirehoseTransformationResult,
  FirehoseTransformationResultRecord,
} from "aws-lambda";

type Data = {
  deviceName: string;
  messageTime: number;
  metrics: Record<string, number>;
};

type EnrichedData = Omit<Data, "messageTime"> & {
  date: string;
  hour: string;
  minute: string;
  second: string;
};

/**
 * 以下の理由から最小のランタイムで実装することを心がけている。
 * - コストメリット
 * - ユーザー入力ではないため、入力内容がブレる可能性が低い
 *
 * 具体的な実装方針は以下の通り。
 * - zodを使わない
 * - loggerライブラリを使わない
 * - 日付ライブラリを使わない
 */
export const handler = async (
  event: Pick<FirehoseTransformationEvent, "records">,
): Promise<FirehoseTransformationResult> => {
  const records = event.records.map((record) => {
    // JSON.parse後のzod検証は行わない。
    const data: Data = JSON.parse(
      Buffer.from(record.data, "base64").toString(),
    );

    const enrichedData = enrich(data);
    const partitionKeys = getPartitionKeys(enrichedData);

    return {
      recordId: record.recordId,
      result: "Ok",
      data: Buffer.from(getJSonLine(enrichedData)).toString("base64"),
      metadata: { partitionKeys },
    } satisfies FirehoseTransformationResultRecord;
  });

  return { records };
};

// ===================================================
// libs

/**
 * データのエンリッチメント
 *
 * JSTタイムゾーンの年月日時分秒をデータに追加している
 * タイムゾーンはLambdaの環境変数で指定している
 * このデータはGlue Tableのスキーマと一致する必要がある
 */
function enrich(data: Data): EnrichedData {
  const { messageTime, deviceName, metrics } = data;

  const datetime = new Date(messageTime);

  const year = datetime.getFullYear().toString();
  const month = formatTwoDigits(datetime.getMonth() + 1);
  const day = formatTwoDigits(datetime.getDate());
  const hour = formatTwoDigits(datetime.getHours());
  const minute = formatTwoDigits(datetime.getMinutes());
  const second = formatTwoDigits(datetime.getSeconds());

  const date = `${year}/${month}/${day}`;

  const enrichedData = {
    deviceName,
    metrics,
    date,
    hour,
    minute,
    second,
  };
  return enrichedData;
}

function getPartitionKeys(enrichedData: EnrichedData) {
  return {
    deviceName: enrichedData.deviceName,
    date: enrichedData.date,
  };
}

/**
 * JSON Line形式の文字列を返す
 * 末尾の改行がとても大事。消さないでね。
 */
function getJSonLine(data: EnrichedData): string {
  return `${JSON.stringify(data)}\n`;
}

function formatTwoDigits(date: number): string {
  return date.toString().padStart(2, "0");
}
