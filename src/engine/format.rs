use std::mem;

pub const HEADER_SIZE: usize = 80;
pub const MAX_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;
pub const GENESIS_HASH: [u8; 16] = [0u8; 16];

#[allow(dead_code)]
pub mod flags {
    pub const CONFIG_CHANGE: u16 = 0x01;
    pub const TOMBSTONE: u16 = 0x02;
    pub const CHECKPOINT: u16 = 0x04;
    pub const END_OF_LOG: u16 = 0x8000;
}

pub const SENTINEL_SIZE: usize = 16;
pub const SENTINEL_MAGIC: [u8; 4] = [0x45, 0x4F, 0x4C, 0x02];
pub const SENTINEL_MAGIC_V1: [u8; 4] = [0x45, 0x4F, 0x4C, 0x00];

#[allow(dead_code)]
pub fn create_sentinel(last_index: u64) -> [u8; SENTINEL_SIZE] {
    let mut sentinel = [0u8; SENTINEL_SIZE];
    sentinel[0..4].copy_from_slice(&SENTINEL_MAGIC);
    sentinel[8..16].copy_from_slice(&last_index.to_le_bytes());
    sentinel
}

#[allow(dead_code)]
pub fn verify_sentinel(sentinel: &[u8; SENTINEL_SIZE], expected_last_index: u64) -> bool {
    if sentinel[0..4] == SENTINEL_MAGIC {
        let stored_index = u64::from_le_bytes([
            sentinel[8], sentinel[9], sentinel[10], sentinel[11],
            sentinel[12], sentinel[13], sentinel[14], sentinel[15],
        ]);
        return stored_index == expected_last_index;
    }
    if sentinel[0..4] == SENTINEL_MAGIC_V1 && expected_last_index <= u32::MAX as u64 {
        let stored_index = u32::from_le_bytes([sentinel[4], sentinel[5], sentinel[6], sentinel[7]]);
        return stored_index as u64 == expected_last_index;
    }
    false
}

#[allow(dead_code)]
pub fn is_sentinel_magic(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && (bytes[0..4] == SENTINEL_MAGIC || bytes[0..4] == SENTINEL_MAGIC_V1)
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LogHeader {
    pub header_checksum: u32,
    pub payload_size: u32,
    pub index: u64,
    pub view_id: u64,
    pub stream_id: u64,
    pub prev_hash: [u8; 16],
    pub payload_hash: [u8; 16],
    pub timestamp_ns: u64,
    pub flags: u16,
    pub schema_version: u16,
    pub reserved: u32,
}

const _: () = assert!(mem::size_of::<LogHeader>() == HEADER_SIZE);

impl LogHeader {
    pub fn new(
        index: u64,
        view_id: u64,
        stream_id: u64,
        prev_hash: [u8; 16],
        payload: &[u8],
        timestamp_ns: u64,
        flags: u16,
        schema_version: u16,
    ) -> Self {
        let payload_hash = compute_payload_hash(payload);
        let payload_size = payload.len() as u32;

        let mut header = LogHeader {
            header_checksum: 0,
            payload_size,
            index,
            view_id,
            stream_id,
            prev_hash,
            payload_hash,
            timestamp_ns,
            flags,
            schema_version,
            reserved: 0,
        };

        header.header_checksum = header.compute_checksum();
        header
    }

    /// Compute CRC32C of header bytes [4..79].
    /// Per log_format.md: Polynomial 0x1EDC6F41, Initial 0xFFFFFFFF, Final XOR 0xFFFFFFFF.
    pub fn compute_checksum(&self) -> u32 {
        let bytes = self.as_bytes();
        // CRC32C of bytes [4..80] (excluding the checksum field itself)
        crc32c::crc32c(&bytes[4..])
    }

    /// Verify the header checksum.
    pub fn verify_checksum(&self) -> bool {
        self.header_checksum == self.compute_checksum()
    }

    /// Convert header to raw bytes.
    ///
    /// # Safety
    /// LogHeader is #[repr(C)] with no padding, so this is safe.
    pub fn as_bytes(&self) -> &[u8; HEADER_SIZE] {
        // SAFETY: LogHeader is #[repr(C)], exactly 80 bytes, no padding.
        // The struct layout matches the on-disk format exactly.
        unsafe { &*(self as *const LogHeader as *const [u8; HEADER_SIZE]) }
    }

    /// Create header from raw bytes.
    /// Reads fields in little-endian order per log_format.md.
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Self {
        let mut header = LogHeader {
            header_checksum: 0,
            payload_size: 0,
            index: 0,
            view_id: 0,
            stream_id: 0,
            prev_hash: [0u8; 16],
            payload_hash: [0u8; 16],
            timestamp_ns: 0,
            flags: 0,
            schema_version: 0,
            reserved: 0,
        };

        header.header_checksum = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        header.payload_size = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        header.index = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        header.view_id = u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19],
            bytes[20], bytes[21], bytes[22], bytes[23],
        ]);
        header.stream_id = u64::from_le_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27],
            bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        header.prev_hash.copy_from_slice(&bytes[32..48]);
        header.payload_hash.copy_from_slice(&bytes[48..64]);
        header.timestamp_ns = u64::from_le_bytes([
            bytes[64], bytes[65], bytes[66], bytes[67],
            bytes[68], bytes[69], bytes[70], bytes[71],
        ]);
        header.flags = u16::from_le_bytes([bytes[72], bytes[73]]);
        header.schema_version = u16::from_le_bytes([bytes[74], bytes[75]]);
        header.reserved = u32::from_le_bytes([bytes[76], bytes[77], bytes[78], bytes[79]]);

        header
    }

    #[allow(dead_code)]
    pub fn is_zero(&self) -> bool { self.as_bytes().iter().all(|&b| b == 0) }
}

pub fn compute_payload_hash(payload: &[u8]) -> [u8; 16] {
    let hash = blake3::hash(payload);
    let mut truncated = [0u8; 16];
    truncated.copy_from_slice(&hash.as_bytes()[..16]);
    truncated
}

/// BLAKE3(Header[4..80] || Payload)[0..16]
pub fn compute_chain_hash(header: &LogHeader, payload: &[u8]) -> [u8; 16] {
    let header_bytes = header.as_bytes();
    let mut hasher = blake3::Hasher::new();
    hasher.update(&header_bytes[4..]);
    hasher.update(payload);
    let hash = hasher.finalize();
    let mut truncated = [0u8; 16];
    truncated.copy_from_slice(&hash.as_bytes()[..16]);
    truncated
}

pub fn calculate_padding(payload_size: u32) -> usize { (8 - (payload_size as usize % 8)) % 8 }

pub const LOG_METADATA_SIZE: usize = 64;
pub const LOG_MAGIC: [u8; 4] = [0x50, 0x4C, 0x4F, 0x47];
pub const LOG_VERSION: u32 = 1;

/// 64-byte header at file start. base_index=0 for fresh logs, snapshot+1 for truncated.
#[derive(Clone, Copy, Debug)]
pub struct LogMetadata {
    pub magic: [u8; 4],
    pub version: u32,
    pub base_index: u64,
    pub base_prev_hash: [u8; 16],
    pub checksum: u32,
}

impl LogMetadata {
    pub fn new_empty() -> Self {
        let mut meta = LogMetadata {
            magic: LOG_MAGIC,
            version: LOG_VERSION,
            base_index: 0,
            base_prev_hash: GENESIS_HASH,
            checksum: 0,
        };
        meta.checksum = meta.compute_checksum();
        meta
    }

    pub fn new_truncated(base_index: u64, base_prev_hash: [u8; 16]) -> Self {
        let mut meta = LogMetadata {
            magic: LOG_MAGIC,
            version: LOG_VERSION,
            base_index,
            base_prev_hash,
            checksum: 0,
        };
        meta.checksum = meta.compute_checksum();
        meta
    }

    pub fn compute_checksum(&self) -> u32 {
        let bytes = self.as_bytes();
        let mut hasher = crc32c::crc32c(&bytes[0..32]);
        crc32c::crc32c_append(hasher, &bytes[36..64])
    }

    pub fn verify_checksum(&self) -> bool { self.checksum == self.compute_checksum() }
    pub fn verify_magic(&self) -> bool { self.magic == LOG_MAGIC }

    pub fn as_bytes(&self) -> [u8; LOG_METADATA_SIZE] {
        let mut bytes = [0u8; LOG_METADATA_SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.base_index.to_le_bytes());
        bytes[16..32].copy_from_slice(&self.base_prev_hash);
        bytes[32..36].copy_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8; LOG_METADATA_SIZE]) -> Self {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);

        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let base_index = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        let mut base_prev_hash = [0u8; 16];
        base_prev_hash.copy_from_slice(&bytes[16..32]);

        let checksum = u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);

        LogMetadata {
            magic,
            version,
            base_index,
            base_prev_hash,
            checksum,
        }
    }
}

pub fn frame_size(payload_size: u32) -> usize {
    HEADER_SIZE + payload_size as usize + calculate_padding(payload_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(mem::size_of::<LogHeader>(), 80);
    }

    #[test]
    fn test_padding_calculation() {
        assert_eq!(calculate_padding(0), 0);
        assert_eq!(calculate_padding(1), 7);
        assert_eq!(calculate_padding(7), 1);
        assert_eq!(calculate_padding(8), 0);
        assert_eq!(calculate_padding(9), 7);
    }

    #[test]
    fn test_checksum_roundtrip() {
        let payload = b"test payload";
        let timestamp_ns = 1_000_000_000u64; // 1 second since epoch
        let header = LogHeader::new(
            0,
            1,
            0,
            GENESIS_HASH,
            payload,
            timestamp_ns,
            0,
            1,
        );
        assert!(header.verify_checksum());
        assert_eq!(header.timestamp_ns, timestamp_ns);
    }
    
    #[test]
    fn test_sentinel_v2_size() {
        // ENFORCES F7: Sentinel must be 16 bytes for full u64 index
        assert_eq!(SENTINEL_SIZE, 16);
    }
    
    #[test]
    fn test_sentinel_v2_roundtrip() {
        // ENFORCES F7: Sentinel stores full u64 index without truncation
        let sentinel = create_sentinel(12345);
        assert!(verify_sentinel(&sentinel, 12345));
        assert!(!verify_sentinel(&sentinel, 12346));
    }
    
    #[test]
    fn test_sentinel_v2_large_index() {
        // ENFORCES F7: Sentinel handles indices > u32::MAX
        let large_index: u64 = (u32::MAX as u64) + 1000;
        let sentinel = create_sentinel(large_index);
        assert!(verify_sentinel(&sentinel, large_index));
        
        // Verify it doesn't match truncated value
        let truncated = large_index as u32 as u64;
        assert!(!verify_sentinel(&sentinel, truncated));
    }
    
    #[test]
    fn test_sentinel_v2_max_index() {
        // ENFORCES F7: Sentinel handles u64::MAX - 1 (max valid index)
        let max_index = u64::MAX - 1;
        let sentinel = create_sentinel(max_index);
        assert!(verify_sentinel(&sentinel, max_index));
    }
    
    #[test]
    fn test_sentinel_magic_detection() {
        // Test is_sentinel_magic for v1 and v2
        assert!(is_sentinel_magic(&SENTINEL_MAGIC));
        assert!(is_sentinel_magic(&SENTINEL_MAGIC_V1));
        assert!(!is_sentinel_magic(&[0x00, 0x00, 0x00, 0x00]));
        assert!(!is_sentinel_magic(&[0xFF, 0xFF, 0xFF, 0xFF]));
    }
}
