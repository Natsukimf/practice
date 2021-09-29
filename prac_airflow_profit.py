# リスト6-1. 各種Pythonモジュールのインポート
import datetime
import os

import airflow
from airflow.contrib.operators import bigquery_operator, \
    bigquery_table_delete_operator, gcs_to_bq
import pendulum


# リスト6-2. DAG内のオペレータ共通のパラメータの定義
# DAG内のオペレータ共通のパラメータを定義する。
default_args = {
    'owner': 'gcpbook',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,#失敗時のリトライ回数
    'retry_delay': datetime.timedelta(minutes=5),
    # DAG作成日の午前2時(JST)を開始日時とする。
    'start_date': pendulum.today('Asia/Tokyo').add(hours=2)
}

# リスト6-3. DAGの定義
# DAGを定義する。
with airflow.DAG(
        'profit_analysis', #命名を変える。DAGの名前となる所
        default_args=default_args,
        # 日次でDAGを実行する。
        schedule_interval=datetime.timedelta(days=1),
        catchup=False) as dag:

    # リスト6-4. GCS上の売上ヒストリーをBQに取り込む
    # Cloud Storage上のユーザ行動ログをBigQueryの作業用テーブルへ
    # GCSto BQへのオペレーターを使用
    # 取り込むタスクを定義する。
    load_receipt = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_receipt',
        bucket=os.environ.get('PROJECT_ID') + '-gcpbook-ch5',
        source_objects=['data/receipt.csv'],
        skip_leading_rows=1,#先頭行を飛ばす
        source_format='CSV',#教科書ではJsonだが、今回はCSVだから
        create_disposition='CREATE_IF_NEEDED',
        destination_project_dataset_table='gcpbook_ch5.receipt',
        write_disposition='WRITE_TRUNCATE',#洗い替え
        autodetect=True,#スキーマを自動判定させる
    )

    # 追加要素. GCS上の商品マスタをBQに取り込む
    # GCSto BQへのオペレーターを使用
    load_product = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_product',
        bucket=os.environ.get('PROJECT_ID') + '-gcpbook-ch5',
        source_objects=['data/product.csv'],
        skip_leading_rows=1,#先頭行を飛ばす
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        destination_project_dataset_table='gcpbook_ch5.product',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # リスト6-5. gcpbook_ch5.salesテーブルへの書き込みタスクの定義
    # BigQueryの売上ヒストリー（receipt）と商品マスタ結合して、利益単価も算出
    #SQL文はここにあるが、複雑化するとディレクトリを分けて書いても良いかも
    # テーブルへ書き込むタスクを定義する。
    # BQオペレーターを使用して、BQ内でのSQL処理を命令
    sales = bigquery_operator.BigQueryOperator(
        task_id='sales',
        use_legacy_sql=False,
        sql="""
            create or replace table  gcpbook_ch5.sales as #BQlikeな書き方へ
            SELECT
                r.sales_ymd
                ,r.product_cd
                ,r.quantity
                ,r.amount
                ,p.category_major_cd
                ,p.unit_price - p.unit_cost as margin
            FROM `mlsystem-project.gcpbook_ch5.receipt` r #joinしているだけ
                left join `mlsystem-project.gcpbook_ch5.product` p
                on r.product_cd = p.product_cd
        """
    )

    # リスト6-5. gcpbook_ch5.profitというデータマートを作成
    # BQオペレーターを使用して、BQ内でのSQL処理を命令
    profit = bigquery_operator.BigQueryOperator(
        task_id='profit',
        use_legacy_sql=False,
        sql="""
            create or replace table  gcpbook_ch5.profit as #BQlikeな書き方へ
            SELECT
                substr(cast(sales_ymd as string) ,1,6) as month #月別の粒度へ変換
                ,category_major_cd　#大項目の商品別
                ,sum((margin*quantity)) as profit　#利益を合計
            FROM gcpbook_ch5.sales
            group by 1,2
        """
    )


    # リスト6-7. タスクの依存関係の定義
    # 各タスクの依存関係を定義する。
    load_receipt >> sales >> profit
    load_product >> sales >> profit
