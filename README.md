# nebula

Just
1. start SRS `java -jar ./source-record-storage/target/source-record-storage-1.0.0-SNAPSHOT-fat.jar`

2. start SRM `java -jar ./source-record-manager/target/source-record-manager-1.0.0-SNAPSHOT-fat.jar`

3. run import `curl -X POST -D - -w '\n' -H "Content-type: application/octet-stream" --data-binary @./30_000_records.mrc http://localhost:8080/import/rawMARCv7`

Enjoy