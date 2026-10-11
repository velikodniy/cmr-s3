// Run this test manually after the shell loads .env into its environment:
// cargo test -p cmr-api --test live -- --ignored --nocapture

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use cmr_api::{
    ByteRange, Client, CloudApi, ContentHash, Download, Error, ObjectType, Result, UploadedContent,
};
use futures_util::TryStreamExt;

fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Integrity(message))
    }
}

async fn collect(download: Download) -> Result<Vec<u8>> {
    download
        .stream
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
}

async fn absent(client: &Client, path: &str) -> Result<()> {
    match client.stat(path).await {
        Err(Error::NotFound(_)) => Ok(()),
        Ok(_) => Err(Error::Protocol("the temporary path still exists")),
        Err(error) => Err(error),
    }
}

async fn metadata_matches(client: &Client, path: &str, content: UploadedContent) -> Result<()> {
    let item = client.stat(path).await?;
    require(
        matches!(item.object_type, ObjectType::File(file) if file.size == content.size && file.hash == content.hash),
        "the live metadata differs from the uploaded content",
    )
}

async fn exercise(client: &Client, root: &str) -> Result<()> {
    client.mkdir(root).await?;
    require(
        matches!(client.stat(root).await?.object_type, ObjectType::Folder(_)),
        "the temporary item is not a folder",
    )?;
    eprintln!("Folder creation passed");

    let path = format!("{root}/file é +%.bin");
    let bytes = Bytes::from(
        (0..65_536)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let uploaded = client.upload(bytes.clone()).await?;
    require(
        uploaded.hash == ContentHash::compute(&bytes) && uploaded.size == bytes.len() as u64,
        "the live upload result differs",
    )?;
    eprintln!("Memory upload and hash comparison passed");

    client.bind(&path, uploaded, SystemTime::now()).await?;
    metadata_matches(client, &path, uploaded).await?;
    eprintln!("Binding and file metadata passed");

    let items = client.list(root).try_collect::<Vec<_>>().await?;
    require(
        items.iter().any(|item| item.home == path),
        "the live folder list omits the file",
    )?;
    eprintln!("Folder listing passed");

    require(
        collect(client.download(&path, None).await?).await? == bytes,
        "the complete live download differs",
    )?;
    eprintln!("Complete download and integrity validation passed");

    let range = ByteRange::new(17, 1_040)?;
    require(
        collect(client.download(&path, Some(range)).await?).await? == bytes[17..=1_040],
        "the live range download differs",
    )?;
    eprintln!("Range download passed");

    let replacement = Bytes::from_static(b"cmr-api live overwrite content");
    let uploaded = client.upload(replacement.clone()).await?;
    client.bind(&path, uploaded, SystemTime::now()).await?;
    metadata_matches(client, &path, uploaded).await?;
    require(
        collect(client.download(&path, None).await?).await? == replacement,
        "the live overwrite download differs",
    )?;
    require(
        client.list(root).try_collect::<Vec<_>>().await?.len() == 1,
        "the live overwrite created an extra item",
    )?;
    eprintln!("Overwrite binding passed");

    for size in [0, 1, 20, 21] {
        let small_path = format!("{root}/small-{size}.bin");
        let bytes = Bytes::from((0..size).collect::<Vec<u8>>());
        let content = client.upload(bytes.clone()).await?;
        client.bind(&small_path, content, SystemTime::now()).await?;
        metadata_matches(client, &small_path, content).await?;
        require(
            collect(client.download(&small_path, None).await?).await? == bytes,
            "the small live download differs",
        )?;
        eprintln!("Upload, binding, and download passed for {size} bytes");
    }

    client.remove(&path).await?;
    absent(client, &path).await?;
    eprintln!("File removal passed");
    Ok(())
}

#[tokio::test]
#[ignore = "Manual live test. Load CMR_LOGIN and CMR_TOKEN from .env."]
async fn live_cloud_round_trip() -> Result<()> {
    let login = std::env::var("CMR_LOGIN").map_err(|_| Error::InvalidCredentials)?;
    let password = std::env::var("CMR_TOKEN").map_err(|_| Error::InvalidCredentials)?;
    let client = Client::new(login, password)?;
    let root = format!("/cmr-api-live-{:032x}", rand::random::<u128>());
    absent(&client, &root).await?;
    eprintln!("Authentication passed. Temporary folder: {root}");

    let outcome = tokio::time::timeout(Duration::from_secs(180), exercise(&client, &root))
        .await
        .unwrap_or(Err(Error::Timeout));
    if let Err(error) = &outcome {
        eprintln!("Live operation failed: {error}");
    }

    // The initial metadata request established that this random path was absent.
    // Cleanup also covers a lost response from folder creation or binding.
    let cleanup = async {
        client.remove(&root).await?;
        absent(&client, &root).await
    }
    .await;
    if let Err(error) = &cleanup {
        eprintln!("Cleanup failed for {root}: {error}");
    }
    cleanup?;
    eprintln!("Temporary folder cleanup passed");
    outcome
}
