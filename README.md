lakehouse/
├─ .gitattributes  .gitignore  .env.example  .env(local)  README.md  docker-compose.yml
├─ config/settings.env
├─ docker/
│  ├─ postgres/init/01_create_databases.sql
│  ├─ tools/Dockerfile, requirements.txt
│  ├─ spark/Dockerfile
│  └─ trino/config.properties, jvm.config, catalog/lakehouse.properties
├─ scripts/init_buckets.py
├─ jobs/common/__init__.py, spark_session.py · jobs/smoke_test.py
├─ docs/
└─ data/landing/   (gitignored: CSV giải nén ở đây, phần 2)