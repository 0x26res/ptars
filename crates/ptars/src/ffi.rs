//! Hand-written [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! structs.
//!
//! These are defined here rather than re-exported from arrow-rs so that this
//! crate's public API carries no arrow types: the C Data Interface ABI is
//! frozen by the Arrow specification, so any arrow implementation (any
//! arrow-rs version, pyarrow, nanoarrow, ...) can produce and consume them.
//!
//! To pass data in from arrow-rs, transmute its FFI structs into these (both
//! are `#[repr(C)]` layouts of the same specification):
//!
//! ```ignore
//! let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array.to_data())?;
//! let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
//! let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };
//! ```
//!
//! And the reverse to read results back with your own arrow version.

use std::ffi::c_void;
use std::os::raw::c_char;

/// `ArrowSchema` from the Arrow C Data Interface specification.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

/// `ArrowArray` from the Arrow C Data Interface specification.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

impl ArrowSchema {
    /// An empty (released) schema, suitable as a placeholder to move into.
    pub fn empty() -> Self {
        Self {
            format: std::ptr::null(),
            name: std::ptr::null(),
            metadata: std::ptr::null(),
            flags: 0,
            n_children: 0,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// Whether the schema has already been released (has no release callback).
    pub fn is_released(&self) -> bool {
        self.release.is_none()
    }
}

impl ArrowArray {
    /// An empty (released) array, suitable as a placeholder to move into.
    pub fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// Whether the array has already been released (has no release callback).
    pub fn is_released(&self) -> bool {
        self.release.is_none()
    }
}

impl Drop for ArrowSchema {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}

impl Drop for ArrowArray {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}

// The C Data Interface mandates that a released structure can be moved and
// freed from any thread; arrow-rs makes the same assumption for its FFI types.
unsafe impl Send for ArrowSchema {}
unsafe impl Send for ArrowArray {}

// Compile-time guarantee that these structs stay layout-compatible with the
// arrow-rs FFI structs this crate transmutes to and from internally.
const _: () = {
    assert!(
        std::mem::size_of::<ArrowSchema>() == std::mem::size_of::<arrow::ffi::FFI_ArrowSchema>()
    );
    assert!(
        std::mem::align_of::<ArrowSchema>() == std::mem::align_of::<arrow::ffi::FFI_ArrowSchema>()
    );
    assert!(std::mem::size_of::<ArrowArray>() == std::mem::size_of::<arrow::ffi::FFI_ArrowArray>());
    assert!(
        std::mem::align_of::<ArrowArray>() == std::mem::align_of::<arrow::ffi::FFI_ArrowArray>()
    );
};

/// Move an arrow-rs FFI array into the public C Data Interface struct.
pub(crate) fn export_array(array: arrow::ffi::FFI_ArrowArray) -> ArrowArray {
    // SAFETY: both types are #[repr(C)] implementations of the same
    // specification struct; the const assertions above check the layout.
    // transmute moves ownership without running either Drop.
    unsafe { std::mem::transmute(array) }
}

/// Move an arrow-rs FFI schema into the public C Data Interface struct.
pub(crate) fn export_schema(schema: arrow::ffi::FFI_ArrowSchema) -> ArrowSchema {
    // SAFETY: see export_array.
    unsafe { std::mem::transmute(schema) }
}

/// Move a public C Data Interface array into the arrow-rs FFI struct.
pub(crate) fn import_array(array: ArrowArray) -> arrow::ffi::FFI_ArrowArray {
    // SAFETY: see export_array.
    unsafe { std::mem::transmute(array) }
}

/// View a public C Data Interface schema as the arrow-rs FFI struct.
pub(crate) fn import_schema_ref(schema: &ArrowSchema) -> &arrow::ffi::FFI_ArrowSchema {
    // SAFETY: same layout (see export_array); lifetime is tied to the input.
    unsafe { &*(schema as *const ArrowSchema as *const arrow::ffi::FFI_ArrowSchema) }
}
