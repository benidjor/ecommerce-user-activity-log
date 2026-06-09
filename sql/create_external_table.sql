-- 사용: spark-sql -f sql/create_external_table.sql 또는 spark.sql 로 실행
-- {{OUTPUT_DIR}} 를 실제 절대경로로 치환 (예: /Users/.../output/activity)
--
-- EXTERNAL TABLE: 데이터 파일(parquet)을 Spark/Hive가 "소유"하지 않고 외부 경로만 참조.
--   DROP TABLE 해도 LOCATION의 실제 데이터는 지워지지 않는다(메타데이터만 제거).
--   적재는 Spark 잡(PartitionWriter)이 하고, 이 테이블은 그 결과를 "노출"만 한다.
--   요구사항 4(External Table 방식): 저장(파일)과 카탈로그(테이블)가 분리돼,
--   추가 기간은 테이블 재생성 없이 새 파티션을 쓰고 MSCK REPAIR로 등록만 하면 된다.
-- 컬럼 목록은 parquet 스키마와 일치(event_date는 파티션 컬럼이라 본문이 아닌 PARTITIONED BY에).
CREATE EXTERNAL TABLE IF NOT EXISTS activity (
  event_time      string,
  event_type      string,
  product_id      bigint,
  category_id     bigint,
  category_code   string,
  brand           string,
  price           double,
  user_id         bigint,
  user_session    string,
  event_time_utc  timestamp,
  event_time_kst  timestamp,
  session_id      string
)
PARTITIONED BY (event_date string)  -- KST 일별 파티션(경로: event_date=YYYY-MM-DD)
STORED AS PARQUET
LOCATION '{{OUTPUT_DIR}}';

-- MSCK REPAIR TABLE: LOCATION 아래의 파티션 디렉터리(event_date=...)를 스캔해
--   메타스토어에 자동 등록. 새 날짜 파티션이 늘어나면 다시 실행해 인식시킨다.
--   (Python 비유: 폴더를 다시 스캔해 인덱스를 갱신하는 것)
--   요구사항 4(추가 기간 처리): incremental로 새 event_date 폴더를 쓴 뒤 이 명령으로 등록만 하면 된다.
MSCK REPAIR TABLE activity;
