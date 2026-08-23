export interface RelayLimits {
  maxStreams: number;
  maxClosedStreamIds: number;
  maxPendingBytes: number;
  maxPendingItems: number;
  maxCarrierBatchBytes: number;
  maxCarrierBatchFrames: number;
}

export const DEFAULT_LIMITS: RelayLimits = Object.freeze({
  maxStreams: 16,
  maxClosedStreamIds: 4096,
  maxPendingBytes: 12 * 1024 * 1024,
  maxPendingItems: 8192,
  maxCarrierBatchBytes: 2 * 1024 * 1024,
  maxCarrierBatchFrames: 4096
});

export const QUEUE_ITEM_COST = 256;
