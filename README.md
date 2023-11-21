## Deploying with helm on prod

Create role with appropriate permissions for spark driver

```
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=<namespace>
```

Deploy secret object with credentials. You can create it from template `secrets.template.yaml`

```
kubectl apply --namespace <namespace> -f secrets.yaml
```

Build airflow docker image with DAGs embedded
```
cd images/airflow
docker build --pull --tag "pweglik/airflow" .
docker push pweglik/airflow:latest
```

Build helm chart
```
cd datalake
helm dependency build
helm install --namespace <namespace> -f values.yaml datalake .
```


## Dremio source config

Type: Nessie (Preview) Source

#### General


URL: http://datalake-nessie:19120/api/v2

Auth: None

#### Storage

Auth Type: AWS ACCESS Token

AWS Aaccess Key: <secret access_key_id>

AWS Acess Secret: <secret secret_access_key>

IAM Role:

AWS Root Path: /alice-data-lake-dev

Encrypt connection: true

Connection Properties:

- fs.s3a.path.style.access true
- fs.s3a.endpoint s3p.cloud.cyfronet.pl
- dremio.s3.compat true

#### Apache Airflow
We create airflow without admin user for safetly purposes.
It is only necessary to do it once we have fresh database. No need to redo after every `helm install/upgrade`
To create admin user:
```
kubectl exec -it --namespace <namespace> <webserver-pod-name>  -- bash 
airflow users delete -u admin
airflow users create --username <username> --password <password> --role Admin --firstname <firstname> --lastname <lastname> --email <email>
```



