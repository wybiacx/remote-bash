use axum_server::tls_rustls::RustlsConfig;
use rcgen::{CertificateParams, DnType, KeyPair, PKCS_ECDSA_P256_SHA256};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

const CERT_DIR_NAME: &str = ".remote-bash";
const CERT_FILE: &str = "cert.pem";
const KEY_FILE: &str = "key.pem";
const FP_FILE: &str = "cert.sha256";

pub struct TlsSetup {
    pub rustls_config: RustlsConfig,
    pub cert_sha256: String,
}

/// Set up TLS: either load user-provided cert or auto-generate a self-signed one.
/// Returns the RustlsConfig and the SHA-256 fingerprint of the certificate.
pub async fn setup_tls() -> Result<TlsSetup, Box<dyn std::error::Error>> {
    let user_cert = std::env::var("MCP_TLS_CERT").ok();
    let user_key = std::env::var("MCP_TLS_KEY").ok();

    match (user_cert, user_key) {
        (Some(cert), Some(key)) => {
            let fp = compute_fingerprint_from_file(&PathBuf::from(&cert))?;
            let rustls_config = RustlsConfig::from_pem_file(&cert, &key).await?;
            Ok(TlsSetup {
                rustls_config,
                cert_sha256: fp,
            })
        }
        _ => {
            // Auto-generate and cache in ~/.remote-bash/
            let dir = cert_dir()?;
            std::fs::create_dir_all(&dir)?;
            let cert_path = dir.join(CERT_FILE);
            let key_path = dir.join(KEY_FILE);
            let fp_path = dir.join(FP_FILE);

            let fp = if cert_path.exists() && key_path.exists() && fp_path.exists() {
                // Reuse existing cert
                std::fs::read_to_string(&fp_path)?.trim().to_string()
            } else {
                // Generate new self-signed cert
                let (cert_pem, key_pem, fp) = generate_self_signed()?;
                std::fs::write(&cert_path, &cert_pem)?;
                std::fs::write(&key_path, &key_pem)?;
                std::fs::write(&fp_path, &fp)?;
                tracing::info!("已生成自签名证书: {}", cert_path.display());
                fp
            };

            let rustls_config = RustlsConfig::from_pem_file(&cert_path, &key_path).await?;
            Ok(TlsSetup {
                rustls_config,
                cert_sha256: fp,
            })
        }
    }
}

fn cert_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let home = dirs::home_dir().ok_or("无法确定 HOME 目录")?;
    Ok(home.join(CERT_DIR_NAME))
}

fn generate_self_signed() -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "remote-bash");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Remote Bash MCP Server");

    let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let cert = params.self_signed(&key_pair)?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    // SHA-256 fingerprint from DER bytes
    let mut hasher = Sha256::new();
    hasher.update(cert.der());
    let hash = hasher.finalize();
    let fp = hex::encode(hash);

    Ok((cert_pem, key_pem, fp))
}

/// Compute SHA-256 fingerprint from a PEM-format certificate file.
fn compute_fingerprint_from_file(path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
    let pem_data = std::fs::read(path)?;
    let mut reader = std::io::BufReader::new(&pem_data[..]);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader).collect::<Result<_, _>>()?;

    if certs.is_empty() {
        return Err("证书文件中未找到证书".into());
    }

    let mut hasher = Sha256::new();
    hasher.update(certs[0].as_ref());
    let hash = hasher.finalize();
    Ok(hex::encode(hash))
}
