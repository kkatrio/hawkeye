use serde::Deserialize;
use serde_json::json;
use yup_oauth2::{InstalledFlowAuthenticator, InstalledFlowReturnMethod};

#[derive(Deserialize)]
struct File {
    id: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Files {
    files: Vec<File>,
    //next_page_token: String,
    //kind: String,
    //incomplete_search: bool,
}

async fn get_token() -> String {
    let app_secret = yup_oauth2::read_application_secret("clientsecret.json")
        .await
        .expect("clientsecret.json");

    let auth =
        InstalledFlowAuthenticator::builder(app_secret, InstalledFlowReturnMethod::Interactive)
            .persist_tokens_to_disk("tokencache.json")
            .build()
            .await
            .unwrap();

    let scopes = &[
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive.readonly",
    ];

    let token = auth.token(scopes).await.unwrap();
    token.token().unwrap().to_string()
}

pub struct GClient {
    client: reqwest::Client,
}

impl GClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub async fn upload(
        &self,
        name: &str,
        path: std::path::PathBuf,
        folder_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: determine mime-type and extension from the path
        let mt = json!({"name": name, "parents": [folder_id]});
        let metadata = reqwest::multipart::Part::text(mt.to_string())
            .mime_str("application/json;charset=UTF-8")?;
        let file_part = reqwest::multipart::Part::file(path)
            .await
            .unwrap()
            .mime_str("image/jpeg")?;
        let form = reqwest::multipart::Form::new()
            .part("metadata", metadata)
            .part("media", file_part);

        let gtoken = get_token().await;
        self.client
            .post("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart")
            .bearer_auth(gtoken)
            .multipart(form)
            .send()
            .await?;
        Ok(())
    }

    // returns (folder_id, file names)
    pub async fn get_uploaded_events(
        &self,
    ) -> Result<(String, Vec<String>), Box<dyn std::error::Error>> {
        // find folder id
        // curl "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.folder' and name='events' and trashed=false" -H "Authorization: Bearer $GTOKEN"
        let gtoken = get_token().await;
        let res = self.client.get("https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.folder' and name='events' and trashed=false").bearer_auth(&gtoken).send().await?;

        let files: Files = serde_json::from_str(&res.text().await?)?;

        // there should be only one events folder
        assert_eq!(files.files.len(), 1);

        let folderid = files.files[0].id.clone();

        // list uploaded events
        // curl "https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}' in parents and trashed=false" \ -H "Authorization: Bearer $GTOKEN"
        let res = self
            .client
            .get(format!(
                "https://www.googleapis.com/drive/v3/files?q='{}' in parents and trashed=false",
                folderid
            ))
            .bearer_auth(&gtoken)
            .send()
            .await?;
        let files: Files = serde_json::from_str(&res.text().await?)?;

        Ok((
            folderid,
            files.files.iter().map(|f| f.name.clone()).collect(),
        ))
    }
}
