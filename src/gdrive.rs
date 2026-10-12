use crate::frigate;
use hyper_util::client::legacy::connect::HttpConnector;
use serde::Deserialize;
use serde_json::json;
use std::fmt;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};
use yup_oauth2::hyper_rustls::HttpsConnector;
use yup_oauth2::{
    authenticator::Authenticator, InstalledFlowAuthenticator, InstalledFlowReturnMethod,
};

pub const RETENTION: i64 = 10 * 24; // hours

#[derive(Debug)]
pub enum GClientError {
    ReqwestError,
    SerdeError(serde_json::Error),
    Io(std::io::Error),
    TokenError(yup_oauth2::Error),
    MissingToken,
    TimestampError(chrono::ParseError),
}

impl std::error::Error for GClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GClientError::ReqwestError => None,
            GClientError::SerdeError(e) => Some(e),
            GClientError::Io(e) => Some(e),
            GClientError::TokenError(e) => Some(e),
            GClientError::MissingToken => None,
            GClientError::TimestampError(e) => Some(e),
        }
    }
}

impl fmt::Display for GClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GClientError::ReqwestError => write!(f, "could not connect to gdrive"),
            GClientError::SerdeError(e) => write!(f, "could not serialize: {e}"),
            GClientError::Io(e) => write!(f, "io error: {e}"),
            GClientError::TokenError(e) => write!(f, "token error: {e}"),
            GClientError::MissingToken => write!(f, "missing token"),
            GClientError::TimestampError(e) => write!(f, "timestamp error: {e}"),
        }
    }
}

impl From<serde_json::Error> for GClientError {
    fn from(e: serde_json::Error) -> Self {
        GClientError::SerdeError(e)
    }
}

impl From<reqwest::Error> for GClientError {
    fn from(_: reqwest::Error) -> Self {
        GClientError::ReqwestError
    }
}

impl From<std::io::Error> for GClientError {
    fn from(e: std::io::Error) -> Self {
        GClientError::Io(e)
    }
}

impl From<yup_oauth2::Error> for GClientError {
    fn from(e: yup_oauth2::Error) -> Self {
        GClientError::TokenError(e)
    }
}

impl From<chrono::ParseError> for GClientError {
    fn from(e: chrono::ParseError) -> Self {
        GClientError::TimestampError(e)
    }
}

#[derive(Deserialize)]
pub struct File {
    pub id: String,
    pub name: String,
    #[serde(rename = "createdTime")]
    pub created_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Files {
    files: Vec<File>,
    //next_page_token: String,
    //kind: String,
    //incomplete_search: bool,
}

const SCOPES: [&str; 2] = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly",
];

type AuthClient = Authenticator<HttpsConnector<HttpConnector>>;

async fn get_auth_client() -> Result<AuthClient, std::io::Error> {
    let app_secret = yup_oauth2::read_application_secret("clientsecret.json").await?;

    InstalledFlowAuthenticator::builder(app_secret, InstalledFlowReturnMethod::Interactive)
        .persist_tokens_to_disk("tokencache.json")
        .build()
        .await
}

struct GDriveAuth {
    auth: AuthClient,
}

impl GDriveAuth {
    async fn new() -> Result<Self, GClientError> {
        let auth = get_auth_client().await?;
        Ok(Self { auth })
    }

    async fn get_token(&self) -> Result<String, GClientError> {
        let token = self.auth.token(&SCOPES).await?;
        token
            .token()
            .map(|t| t.to_owned())
            .ok_or(GClientError::MissingToken)
    }
}

pub struct GClient {
    client: reqwest::Client,
    folderid: String,
    auth: GDriveAuth,
}

impl GClient {
    pub async fn new() -> Result<Self, GClientError> {
        let client = reqwest::Client::new();
        let auth = GDriveAuth::new().await?;
        let folderid = Self::get_folder_id(&client, &auth).await?;
        Ok(Self {
            client,
            folderid,
            auth,
        })
    }

    async fn get_folder_id(
        client: &reqwest::Client,
        auth: &GDriveAuth,
    ) -> Result<String, GClientError> {
        /*
         set Q "mimeType='application/vnd.google-apps.folder' and name='events' and trashed=false"
         curl --get "https://www.googleapis.com/drive/v3/files" \
               --data-urlencode "q=$Q" \
               -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = auth.get_token().await?;
        let res = client.get("https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.folder' and name='events' and trashed=false").bearer_auth(&gtoken).send().await?;
        let files: Files = serde_json::from_str(&res.text().await?)?;

        // there should be only one events folder
        assert_eq!(files.files.len(), 1);

        Ok(files.files[0].id.clone())
    }

    pub async fn upload(
        &self,
        after_event: &frigate::After,
        path: std::path::PathBuf,
    ) -> Result<String, GClientError> {
        // TODO: determine mime-type and extension from the path
        let name = match after_event.get_name() {
            Some(n) => n,
            None => "no-name".to_string(),
        };
        let mt = json!({"name": name, "parents": [self.folderid]});
        let metadata = reqwest::multipart::Part::text(mt.to_string())
            .mime_str("application/json;charset=UTF-8")?;
        let file_part = reqwest::multipart::Part::file(path)
            .await?
            .mime_str("image/jpeg")?;
        let form = reqwest::multipart::Form::new()
            .part("metadata", metadata)
            .part("media", file_part);

        let gtoken = self.auth.get_token().await?;
        self.client
            .post("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart")
            .bearer_auth(gtoken)
            .multipart(form)
            .send()
            .await?;
        Ok(name)
    }

    #[allow(dead_code)]
    async fn get_uploaded_events(&self) -> Result<Vec<String>, GClientError> {
        /*
         set Q "'${FOLDER_ID}' in parents and trashed=false"
         curl --get "https://www.googleapis.com/drive/v3/files" \
               --data-urlencode "q=$Q" \
               -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = self.auth.get_token().await?;

        let res = self
            .client
            .get(format!(
                "https://www.googleapis.com/drive/v3/files?q='{}' in parents and trashed=false",
                self.folderid
            ))
            .bearer_auth(&gtoken)
            .send()
            .await?;

        let files: Files = serde_json::from_str(&res.text().await?)?;
        Ok(files.files.iter().map(|f| f.name.clone()).collect())
    }

    async fn get_uploaded_events_reverse(&self) -> Result<Vec<File>, GClientError> {
        /*
         set Q "'16UmrM5Clb71ZdG28tsFOyos5GyoA_FnC' in parents and trashed=false"
         curl --get "https://www.googleapis.com/drive/v3/files" \
                            --data-urlencode "q=$Q" --data-urlencode "orderBy=createdTime" \
                            -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = self.auth.get_token().await?;
        let res = self
            .client
            .get(format!(
                "https://www.googleapis.com/drive/v3/files?q='{}' in parents and trashed=false&fields=files(id,name,createdTime)&orderBy=createdTime",
                self.folderid
            ))
            .bearer_auth(&gtoken)
            .send()
            .await?;
        let files: Files = serde_json::from_str(&res.text().await?)?;
        info!("listed {} files in gdrive", files.files.len());
        Ok(files.files)
    }

    async fn delete_old(&self) -> Result<u32, GClientError> {
        let files = self.get_uploaded_events_reverse().await?;
        let mut count = 0;
        for f in files {
            if let Some(t) = f.created_time.as_ref() {
                if is_older_than_retention(t)? {
                    debug!("deleting file: {}", f.name);
                    self.delete(&f.id).await?;
                    count += 1;
                } else {
                    debug!("keeping file: {}", f.name);
                }
            } else {
                warn!(
                    "file: {} missing created time. Must delete manually",
                    f.name
                );
            }
        }
        Ok(count)
    }

    async fn delete(&self, fileid: &str) -> Result<(), GClientError> {
        /*
         set FID "1zg3C2XukF0zf56oVeWJOejpBQZwWFLhe"
         curl -X DELETE "https://www.googleapis.com/drive/v3/files/$FID" -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = self.auth.get_token().await?;
        self.client
            .delete(format!(
                "https://www.googleapis.com/drive/v3/files/{}",
                fileid
            ))
            .bearer_auth(&gtoken)
            .send()
            .await?;
        Ok(())
    }
}

fn is_older_than_retention(timestamp: &str) -> Result<bool, chrono::ParseError> {
    // createdTime should be in Utc
    let time = timestamp.parse::<chrono::DateTime<chrono::Utc>>()?;
    let days_ago = chrono::Utc::now() - chrono::Duration::hours(RETENTION);
    Ok(time < days_ago)
}

#[instrument(skip(gclient))]
pub async fn delete_old(gclient: Arc<GClient>) {
    let mut interval = 6;
    loop {
        info!("deleting old events");
        match gclient.delete_old().await {
            Ok(deleted) => {
                info!("deleted: {deleted} files");

                // list gdrive api fetches 100 events
                if deleted == 100 {
                    warn!("more than 100 files to delete -- changing delete interval to 1 hour.");
                    interval = 1;
                } else if deleted < 100 && interval == 1 {
                    warn!("deleted less than 100 -- changing delete interval back to 6 hours.");
                    interval = 6;
                }
            }
            Err(e) => error!(error = ?e, "error deleting old events"),
        }
        tokio::time::sleep(std::time::Duration::from_hours(interval)).await;
    }
}
