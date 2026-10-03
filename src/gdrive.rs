use hyper_util::client::legacy::connect::HttpConnector;
use serde::Deserialize;
use serde_json::json;
use std::fmt;
use tracing::{debug, error, info};
use yup_oauth2::hyper_rustls::HttpsConnector;
use yup_oauth2::{
    authenticator::Authenticator, InstalledFlowAuthenticator, InstalledFlowReturnMethod,
};

#[derive(Debug)]
pub enum GClientError {
    ReqwestError,
    SerdeError(serde_json::Error),
}

impl std::error::Error for GClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GClientError::ReqwestError => None,
            GClientError::SerdeError(e) => Some(e),
        }
    }
}

impl fmt::Display for GClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GClientError::ReqwestError => write!(f, "could not connect to gdrive"),
            GClientError::SerdeError(e) => write!(f, "could not serialize: {e}"),
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

async fn get_auth_client() -> AuthClient {
    let app_secret = yup_oauth2::read_application_secret("clientsecret.json")
        .await
        // TODO: handle error
        .expect("clientsecret.json");

    InstalledFlowAuthenticator::builder(app_secret, InstalledFlowReturnMethod::Interactive)
        .persist_tokens_to_disk("tokencache.json")
        .build()
        .await
        // TODO: handle error
        .unwrap()
}

struct GDriveAuth {
    auth: AuthClient,
}

impl GDriveAuth {
    async fn new() -> Result<Self, GClientError> {
        let auth = get_auth_client().await;
        Ok(Self { auth })
    }

    async fn get_token(&self) -> String {
        let token = self.auth.token(&SCOPES).await.unwrap();
        // TODO: handle error
        token.token().unwrap().to_string()
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
        let gtoken = auth.get_token().await;
        let res = client.get("https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.folder' and name='events' and trashed=false").bearer_auth(&gtoken).send().await?;
        let files: Files = serde_json::from_str(&res.text().await?)?;

        // there should be only one events folder
        assert_eq!(files.files.len(), 1);

        Ok(files.files[0].id.clone())
    }

    pub async fn upload(&self, name: &str, path: std::path::PathBuf) -> Result<(), GClientError> {
        // TODO: determine mime-type and extension from the path
        let mt = json!({"name": name, "parents": [self.folderid]});
        let metadata = reqwest::multipart::Part::text(mt.to_string())
            .mime_str("application/json;charset=UTF-8")?;
        let file_part = reqwest::multipart::Part::file(path)
            .await
            .unwrap()
            .mime_str("image/jpeg")?;
        let form = reqwest::multipart::Form::new()
            .part("metadata", metadata)
            .part("media", file_part);

        let gtoken = self.auth.get_token().await;
        self.client
            .post("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart")
            .bearer_auth(gtoken)
            .multipart(form)
            .send()
            .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_uploaded_events(&self) -> Result<Vec<String>, GClientError> {
        /*
         set Q "'${FOLDER_ID}' in parents and trashed=false"
         curl --get "https://www.googleapis.com/drive/v3/files" \
               --data-urlencode "q=$Q" \
               -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = self.auth.get_token().await;

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

    pub async fn get_uploaded_events_reverse(&self) -> Result<Vec<File>, GClientError> {
        /*
         set Q "'16UmrM5Clb71ZdG28tsFOyos5GyoA_FnC' in parents and trashed=false"
         curl --get "https://www.googleapis.com/drive/v3/files" \
                            --data-urlencode "q=$Q" --data-urlencode "orderBy=createdTime" \
                            -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = self.auth.get_token().await;
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

    pub async fn delete_old(&self, files: &Vec<File>) -> u32 {
        let mut count = 0;
        for f in files {
            let t = f.created_time.as_ref().unwrap();
            match is_older_than_retention(t) {
                Ok(true) => {
                    debug!("deleting file: {}", f.name);
                    if let Err(e) = self.delete(&f.id).await {
                        error!("error when deleting file: {e}");
                    }
                    count += 1;
                }
                Ok(false) => {
                    debug!("keeping file: {}", f.name);
                }
                Err(e) => {
                    error!("could not parse fetched timestamp: {e}")
                }
            }
        }
        count
    }

    async fn delete(&self, fileid: &str) -> Result<(), GClientError> {
        /*
         set FID "1zg3C2XukF0zf56oVeWJOejpBQZwWFLhe"
         curl -X DELETE "https://www.googleapis.com/drive/v3/files/$FID" -H "Authorization: Bearer $TOKEN"
        */
        let gtoken = self.auth.get_token().await;
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
    let two_days_ago = chrono::Utc::now() - chrono::Duration::hours(crate::RETENTION);
    Ok(time < two_days_ago)
}
