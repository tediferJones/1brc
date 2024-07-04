export interface CityRecord {
  min: number,
  max: number,
  count: number,
  total: number,
}

export type MapResult = Map<string, CityRecord>

export interface MiniMapResult {
  first: Buffer,
  last: Buffer,
  result: MapResult,
  i: number,
}

export interface WorkerReq {
  filePath: string,
  i: number,
  bufSize: number
}
