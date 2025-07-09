import { leaderState } from "./leader";

export function getData(call: any, callback: any) {
  const { key } = call.request;
  getDataFromDatabase(key)
    .then((val) => {
      if (val) return callback(null, { key, value: val });
      callback(null, { key, value: "" });
    })
    .catch(() => undefined);
}

export async function getDataFromDatabase(
  key: string
): Promise<string | undefined> {
  return await leaderState.read(key);
}
