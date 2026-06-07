package com.activitylog

import org.apache.spark.sql.{DataFrame, SparkSession}

// object: Scala의 싱글턴 객체(singleton). Python의 모듈 수준 함수와 유사하게,
//   인스턴스 생성 없이 WauQueries.wauUsers(spark) 형태로 직접 호출한다.
object WauQueries {

  // val wauUsersSql: String : 변경 불가능한(immutable) SQL 문자열 상수.
  //   Python의 WAU_USERS_SQL = "SELECT ..." 와 동일 역할.
  //
  // """...""".stripMargin : Scala 멀티라인 문자열(triple-quote).
  //   각 줄 앞의 '|'(파이프 문자)는 들여쓰기 마커 — stripMargin이 '|' 이전 공백을 제거해
  //   깔끔한 SQL 문자열을 만든다. Python의 textwrap.dedent("""...""")와 유사.
  //
  // ─── SQL 설명 ───────────────────────────────────────────────────────────────
  // date_trunc('week', to_date(event_date))
  //   → to_date: 문자열 event_date를 Date 타입으로 변환 (Python: datetime.date.fromisoformat)
  //   → date_trunc('week', ...): ISO 주 기준(월요일 시작) 주의 첫 번째 날(월요일)을 반환.
  //     예) '2019-10-09'(수) → '2019-10-07'(월). Python의 pd.Grouper(freq='W-MON')와 유사.
  // COUNT(DISTINCT user_id): 주별 고유 사용자 수. Python의 nunique() 또는 COUNTD()와 동일.
  // GROUP BY 1 ORDER BY 1: SELECT 절의 첫 번째 컬럼(week_start)으로 그룹화 및 정렬.
  val wauUsersSql: String =
    """SELECT date_trunc('week', to_date(event_date)) AS week_start,
      |       COUNT(DISTINCT user_id) AS wau_users
      |FROM activity GROUP BY 1 ORDER BY 1""".stripMargin

  // ─── SQL 설명 ───────────────────────────────────────────────────────────────
  // wauUsersSql과 구조 동일. COUNT(DISTINCT session_id) 로 주별 고유 세션 수를 계산.
  val wauSessionsSql: String =
    """SELECT date_trunc('week', to_date(event_date)) AS week_start,
      |       COUNT(DISTINCT session_id) AS wau_sessions
      |FROM activity GROUP BY 1 ORDER BY 1""".stripMargin

  // spark.sql(wauUsersSql): "activity" 임시 뷰에 대해 SQL을 실행하고 DataFrame 반환.
  //   Python의 spark.sql("SELECT ...") 과 완전히 동일한 API.
  // DataFrame: Spark의 분산 데이터셋(Python의 pyspark.sql.DataFrame과 동일 개념).
  def wauUsers(spark: SparkSession): DataFrame    = spark.sql(wauUsersSql)
  def wauSessions(spark: SparkSession): DataFrame = spark.sql(wauSessionsSql)
}
