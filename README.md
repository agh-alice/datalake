### Dremio source config

Type: Nessie (Preview) Source

#### General


URL: http://nessie:19120/api/v2

Auth: None

#### Storage

Auth Type: AWS ACCESS Token

AWS Aaccess Key: admin

AWS Acess Secret: password

IAM Role:

AWS Root Path: /warehouse

Encrypt connection: false

Connection Properties:

- fs.s3a.path.style.access true
- fs.s3a.endpoint minio:9000
- dremio.s3.compat true
