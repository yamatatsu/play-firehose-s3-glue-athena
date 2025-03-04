import { randomUUID } from 'node:crypto';
import { IoTDataPlane } from '@aws-sdk/client-iot-data-plane'

const ioTDataPlane = new IoTDataPlane();

type Data = {
  deviceName: string;
  messageTime: number;
  metrics: Record<string, number>;
};

/**
 * JSTでパーティションが作成されることを確認するために、
 * JSTの2025年1月1日を基準日とする
 */
let date = new Date('2025-01-01T00:00:00+09:00');
const endDate = new Date('2025-04-01T00:00:00+09:00');

export const handler = async (): Promise<void> => {
  /** 実行都度、適当なデバイス名でデータを送信する */
  const deviceName = `device-${randomUUID()}`;

  while (date < endDate) {
    console.log('Published data:', date);
    const data: Data = {
      deviceName,
      messageTime: date.getTime(),
      metrics: {
        metric1: Math.random(),
        metric2: Math.random(),
      },
    };
    await ioTDataPlane.publish({
      topic: 'iot-data',
      payload: JSON.stringify(data),
    });

    date = new Date(date.getTime() + Math.random() * 1000 * 60 * 60);
  }
};
