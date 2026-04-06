#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HlsKernelConfig {
    pub big_kernels: usize,
    pub little_kernels: usize,
}

impl HlsKernelConfig {
    pub const fn new(big_kernels: usize, little_kernels: usize) -> Self {
        Self {
            big_kernels,
            little_kernels,
        }
    }
}

impl Default for HlsKernelConfig {
    fn default() -> Self {
        Self::new(4, 10)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HlsEdgeConfig {
    pub edge_prop_bits: u32,
    pub edge_prop_widths: Vec<u32>,
    pub edge_weight_bits: u32,
    pub edge_weight_lsb: u32,
    pub edge_weight_shift: i32,
    pub edges_per_word: u32,
    pub big_pe: u32,
    pub big_log_pe: u32,
    pub little_pe: u32,
    /// Bit width for compressed local destination IDs (default 32).
    pub local_id_bits: u32,
    /// When true, edge properties are compacted into the upper bits of the
    /// lower 32-bit destination lane and the physical packed payload is fixed
    /// at 64 bits. This is used by the DDR flow.
    pub compact_edge_payload: bool,
    /// When true, reduce uses 0 as "empty" sentinel (URAM inits to 0).
    /// When false, reduce uses identity value with explicit init loop.
    pub zero_sentinel: bool,
    /// When true, scatter may safely overflow positive-infinity source props
    /// to zero because the downstream path treats zero as an empty sentinel.
    /// This is only valid for the DDR flow.
    pub allow_scatter_inf_overflow_to_zero: bool,
}

impl HlsEdgeConfig {
    pub fn payload_bits(&self) -> u32 {
        if self.compact_edge_payload {
            64
        } else {
            self.edge_prop_bits + 64
        }
    }

    pub fn edge_prop_payload_lsb(&self) -> u32 {
        if self.compact_edge_payload {
            self.local_id_bits
        } else {
            64
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HlsNodeConfig {
    pub node_prop_bits: u32,
    pub node_prop_int_bits: u32,
    pub node_prop_signed: bool,
    pub dist_per_word: u32,
    pub log_dist_per_word: u32,
    pub distances_per_reduce_word: u32,
}
