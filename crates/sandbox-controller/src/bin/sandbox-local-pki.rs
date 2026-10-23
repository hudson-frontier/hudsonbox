//! Prepare private development TLS identities without starting services or contacting storage.
#[cfg(not(unix))]
fn main() {
    eprintln!("Local certificate preparation requires Unix file permissions.");
    std::process::exit(1);
}

#[cfg(unix)]
fn main() {
    native::run();
}

#[cfg(unix)]
mod native {
    use anyhow::{Context, ensure};
    use clap::Parser;
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair, KeyUsagePurpose,
    };
    use sandbox_protocol::{HostId, Id};
    use serde::Serialize;
    use sha2::{Digest, Sha256};
    use std::{
        fs::{self, File, OpenOptions},
        io::Write,
        os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        path::{Path, PathBuf},
    };

    #[derive(Parser)]
    struct Args {
        /// New absolute destination under an existing private, owned canonical directory.
        #[arg(long)]
        directory: PathBuf,
        /// Retained operator host identity; generate a new one when preparing a new host.
        #[arg(long)]
        host_id: Option<HostId>,
    }
    #[derive(Debug, Serialize)]
    struct Authority {
        certificate: PathBuf,
        private_key: PathBuf,
    }
    #[derive(Debug, Serialize)]
    struct Service {
        certificate: PathBuf,
        private_key: PathBuf,
        sha256_der: String,
    }
    #[derive(Debug, Serialize)]
    struct Report {
        version: u32,
        scope: &'static str,
        host_id: HostId,
        expires_unix_seconds: i64,
        ca: Authority,
        api_server: Service,
        host_server: Service,
        controller: Service,
        output_reader: Service,
        file_reader: Service,
        manifest: PathBuf,
    }

    fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn prepare(args: Args) -> anyhow::Result<Report> {
        ensure!(
            args.directory.is_absolute(),
            "absolute destination required"
        );
        let parent = args.directory.parent().context("private parent required")?;
        ensure!(
            parent.canonicalize()? == parent,
            "canonical parent required"
        );
        let metadata = fs::symlink_metadata(parent)?;
        ensure!(
            metadata.is_dir()
                && metadata.uid() == rustix::process::geteuid().as_raw()
                && metadata.mode() & 0o077 == 0,
            "private owned parent required"
        );
        fs::DirBuilder::new().mode(0o700).create(&args.directory)?;
        fs::set_permissions(&args.directory, fs::Permissions::from_mode(0o700))?;
        let directory = &args.directory;
        let host_id = args.host_id.unwrap_or_else(HostId::generate);
        let now = time::OffsetDateTime::now_utc();
        let not_before = now - time::Duration::minutes(1);
        let not_after = now + time::Duration::days(7);
        let mut parameters = CertificateParams::new(Vec::new())?;
        parameters.not_before = not_before;
        parameters.not_after = not_after;
        parameters
            .distinguished_name
            .push(DnType::CommonName, "Hudsonbox local development CA");
        parameters.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        parameters.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate()?;
        let private_key = key.serialize_pem();
        let issuer = CertifiedIssuer::self_signed(parameters, key)?;
        let ca = Authority {
            certificate: directory.join("ca.pem"),
            private_key: directory.join("ca.key"),
        };
        write_private(&ca.certificate, issuer.pem().as_bytes())?;
        write_private(&ca.private_key, private_key.as_bytes())?;

        let leaf = |name: &str, identities: Vec<String>, purpose| -> anyhow::Result<Service> {
            let key = KeyPair::generate()?;
            let mut parameters = CertificateParams::new(identities)?;
            parameters.not_before = not_before;
            parameters.not_after = not_after;
            parameters.distinguished_name.push(DnType::CommonName, name);
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![purpose];
            let certificate = parameters.signed_by(&key, &issuer)?;
            let result = Service {
                certificate: directory.join(format!("{name}.pem")),
                private_key: directory.join(format!("{name}.key")),
                sha256_der: format!("{:x}", Sha256::digest(certificate.der())),
            };
            write_private(&result.certificate, certificate.pem().as_bytes())?;
            write_private(&result.private_key, key.serialize_pem().as_bytes())?;
            Ok(result)
        };
        let report = Report {
            version: 1,
            scope: "local development certificates; no production trust or runtime qualification",
            host_id,
            expires_unix_seconds: not_after.unix_timestamp(),
            ca,
            api_server: leaf(
                "api-server",
                vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
                ExtendedKeyUsagePurpose::ServerAuth,
            )?,
            host_server: leaf(
                "host-server",
                vec![sandbox_supervisor::transport::host_server_name(host_id)],
                ExtendedKeyUsagePurpose::ServerAuth,
            )?,
            controller: leaf(
                "controller",
                vec!["controller.sandbox.internal".into()],
                ExtendedKeyUsagePurpose::ClientAuth,
            )?,
            output_reader: leaf(
                "output-reader",
                vec!["output-reader.sandbox.internal".into()],
                ExtendedKeyUsagePurpose::ClientAuth,
            )?,
            file_reader: leaf(
                "file-reader",
                vec!["file-reader.sandbox.internal".into()],
                ExtendedKeyUsagePurpose::ClientAuth,
            )?,
            manifest: directory.join("manifest.json"),
        };
        write_private(&report.manifest, &serde_json::to_vec_pretty(&report)?)?;
        File::open(directory)?.sync_all()?;
        File::open(parent)?.sync_all()?;
        Ok(report)
    }

    pub(super) fn run() {
        let result = prepare(Args::parse()).and_then(|report| {
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        });
        if result.is_err() {
            eprintln!(
                "Local certificate preparation failed. Use a new destination under a private owned directory; retain any partial output for inspection."
            );
            std::process::exit(1);
        }
    }
}
