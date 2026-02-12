//! File type detection using magic bytes
//!
//! Uses the `infer` crate to detect file types from magic bytes (file headers).
//! This is more accurate than extension-based detection and works even when
//! files are renamed or have missing extensions.
//!
//! Only requires the first few bytes of a file (typically 8KB or less),
//! making it efficient for scanning large filesystems over NFS.

/// Detect the MIME type of a file from its header bytes
///
/// Returns the detected MIME type as a string, or None if the type is unknown.
///
/// # Example
///
/// ```
/// use nfs_walker::content::filetype::detect_file_type;
///
/// // PNG file magic bytes
/// let png_header = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
/// assert_eq!(detect_file_type(png_header), Some("image/png".to_string()));
///
/// // Unknown content
/// let unknown = &[0x00, 0x01, 0x02, 0x03];
/// assert_eq!(detect_file_type(unknown), None);
/// ```
pub fn detect_file_type(header: &[u8]) -> Option<String> {
    infer::get(header).map(|kind| kind.mime_type().to_string())
}

/// Detect the file type and return a human-readable description
///
/// Returns a tuple of (MIME type, description) or None if unknown.
pub fn detect_file_type_with_description(header: &[u8]) -> Option<(String, String)> {
    infer::get(header).map(|kind| {
        let mime = kind.mime_type().to_string();
        let desc = kind.extension().to_uppercase();
        (mime, desc)
    })
}

/// Check if the content is an image file
pub fn is_image(header: &[u8]) -> bool {
    infer::is_image(header)
}

/// Check if the content is a video file
pub fn is_video(header: &[u8]) -> bool {
    infer::is_video(header)
}

/// Check if the content is an audio file
pub fn is_audio(header: &[u8]) -> bool {
    infer::is_audio(header)
}

/// Check if the content is an archive file
pub fn is_archive(header: &[u8]) -> bool {
    infer::is_archive(header)
}

/// Check if the content is a document file
pub fn is_document(header: &[u8]) -> bool {
    infer::is_document(header)
}

/// Check if the content is an application/executable
pub fn is_app(header: &[u8]) -> bool {
    infer::is_app(header)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_png() {
        // PNG magic bytes
        let png_header = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let mime = detect_file_type(png_header);
        assert_eq!(mime, Some("image/png".to_string()));
        assert!(is_image(png_header));
    }

    #[test]
    fn test_detect_jpeg() {
        // JPEG magic bytes
        let jpeg_header = &[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46];
        let mime = detect_file_type(jpeg_header);
        assert_eq!(mime, Some("image/jpeg".to_string()));
        assert!(is_image(jpeg_header));
    }

    #[test]
    fn test_detect_pdf() {
        // PDF magic bytes
        let pdf_header = b"%PDF-1.5";
        let mime = detect_file_type(pdf_header);
        assert_eq!(mime, Some("application/pdf".to_string()));
        assert!(is_document(pdf_header));
    }

    #[test]
    fn test_detect_zip() {
        // ZIP magic bytes (PK\x03\x04)
        let zip_header = &[0x50, 0x4B, 0x03, 0x04];
        let mime = detect_file_type(zip_header);
        assert_eq!(mime, Some("application/zip".to_string()));
        assert!(is_archive(zip_header));
    }

    #[test]
    fn test_detect_elf() {
        // ELF magic bytes
        let elf_header = &[0x7F, 0x45, 0x4C, 0x46, 0x02, 0x01, 0x01, 0x00];
        let mime = detect_file_type(elf_header);
        assert_eq!(mime, Some("application/x-executable".to_string()));
        assert!(is_app(elf_header));
    }

    #[test]
    fn test_detect_mp3() {
        // MP3 magic bytes (ID3v2 header)
        let mp3_header = &[0x49, 0x44, 0x33, 0x04, 0x00, 0x00, 0x00, 0x00];
        let mime = detect_file_type(mp3_header);
        assert_eq!(mime, Some("audio/mpeg".to_string()));
        assert!(is_audio(mp3_header));
    }

    #[test]
    fn test_unknown_content() {
        // Random bytes - unknown file type
        let unknown = &[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let mime = detect_file_type(unknown);
        assert_eq!(mime, None);
    }

    #[test]
    fn test_empty_content() {
        let empty: &[u8] = &[];
        let mime = detect_file_type(empty);
        assert_eq!(mime, None);
    }
}
