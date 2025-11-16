---
title: Kubernetes 1.28 から始める Sidecar Containers
tags:
  - kubernetes
  - lifecycle
private: false
updated_at: '2023-12-04T11:12:44+09:00'
id: b478bdecf8da67f600b9
organization_url_name: donuts_coltd
slide: false
ignorePublish: false
---
ジョブカンアドベントカレンダー 4 日目です.

https://qiita.com/advent-calendar/2023/jobcan

Kubernetes 1.28 で来た待望の Sidecar Containers について語ります.

https://kubernetes.io/blog/2023/08/15/kubernetes-v1-28-release/#sidecar-init-containers

なおこの機能は 12/05 リリースの Kubernetes 1.29 より, Beta 昇格でデフォルト有効化されます. (アドカレの日付明日にしとけばよかった...)


> The SidecarContainers feature has graduated to beta and is enabled by default. (#121579, @gjkim42) [SIG Node]
https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG/CHANGELOG-1.29.md 

## TL;DR

- Kubernetes 1.28 でアルファリリースされた Sidecar Containers は特殊な init Container
- Sidecar Containers は**メインコンテナより前から起動する**
- Sidecar Containers は**Job の完了判定対象とならない**
- 今までスクリプトやアプリケーション自体に入れる必要のあった待機処理/終了処理が不要に

なお当記事内では 「Sidecar」は主にメインで稼働するコンテナの補助や, その通信の中継となるようなコンテナ全般を指すものとします.

## 従来の Sidecar コンテナの問題点

Kubernetes において, 1 つの Pod に対して複数のコンテナを配置することは今までもできましたが, 次のような問題点がありました.

### メインコンテナと並列に起動する

従来の Sidecar はメインコンテナとの間に関係性が定義出来ません.
このため, 特にケアしない場合, Sidecar がメインコンテナより先に起動することを保証できません.

例えば外部への接続 Proxy を担うような Sidecar コンテナの場合, 一時的にメインコンテナの通信が遮断された状態となってしまう等, 微妙に困った状態となることがあります.

![37378851ec4e14e47ba68aa1.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/594776/bf26a287-8f2b-3fb3-fad1-5a36e8314f6e.png)

### Job が終了できない

Job リソースは記述されたコンテナがすべて完了状態となった場合に終了する, バッチ処理等を行うのに適したリソースです.

Sidecar コンテナは基本的にはメインのコンテナの状態に関わらず**稼働し続けます**.
そのため, 安易に Job リソースにて Sidecar コンテナを利用すると, Sidecar のみが稼働し続ける謎の Job が実行中判定で残り続けることになります.

![15bf6c4a1c0a788421a6dfd5.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/594776/04fd2f79-6fae-3963-1bc3-8ec7667695a8.png)

## 従来の対処方法

上記問題についてはすべてコンテナ内か, コンテナの実行スクリプトでなんとかする必要がありました.

例として Sidecar コンテナに Cloud SQL Proxy を配置していた場合を見てみます.

https://github.com/GoogleCloudPlatform/cloud-sql-proxy

```yaml
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "sidecar-test"
spec:
  template:
    spec:
      restartPolicy: "Never"
      containers:
        - name: main
          image: "main:latest"
          imagePullPolicy: "Never"
          command: ["/bin/bash", "-c"]
          args:
            - |
              (until curl -o - ${MYSQL_HOST}:8090/readiness &>/dev/null; do sleep 1; done)
              trap "touch /tmp/pod/terminated" EXIT
              /main
          env:
            - name: MYSQL_HOST
              value: "0.0.0.0"
          volumeMounts:
            - name: "tmp-pod"
              mountPath: "/tmp/pod"
        - name: cloud-sql-proxy
          image: "gcr.io/cloudsql-docker/gce-proxy:1.33.13-alpine"
          command: ["/bin/sh", "-c"]
          args:
            - |
              /cloud_sql_proxy \
                -instances=${PROJECT}:${REGION}:${INSTANCE_NAME}=tcp:0.0.0.0:3306 \
                -term_timeout=30s \
                -use_http_health_check \
                -health_check_port=8090 &
              CHILD_PID=$!
              (while true; do if [[ -f "/tmp/pod/terminated" ]]; then kill -TERM ${CHILD_PID}; break; fi; sleep 1; done) &
              wait ${CHILD_PID}
              if [[ -f "/tmp/pod/terminated" ]]; then exit 0; else exit 1; fi
          ports:
            - name: "mysql-port"
              containerPort: 3306
          volumeMounts:
            - name: "tmp-pod"
              mountPath: "/tmp/pod"
              readOnly: true
      volumes:
        - name: "tmp-pod"
          emptyDir: {}
```

図示するとこんな感じになります.

![d056e70066f381a576946363.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/594776/2c2583e7-570a-04ea-95ef-54f9b2808677.png)

こんなことをする他なかったんだという雰囲気だけでも伝わっていれば幸いです.
「YAML で宣言的に管理」という Kubernetes のコンセプトからかけ離れた手続き記述.

Cloud SQL Proxy の方のコードは Kubernetes の以前の Issue にあるものを参考にしたものです.

https://github.com/kubernetes/kubernetes/issues/25908#issuecomment-365924958

<details>
<summary>Cloud SQL Proxy v2 の場合...</summary>

Cloud SQL Proxy v2 シャットダウンを行うエンドポイントを起動するオプション`quitquitquit`が追加され, 該当のエンドポイントを叩くように記述することで記述を簡略化できます.

```yaml
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "sidecar-test"
spec:
  template:
    spec:
      restartPolicy: "Never"
      containers:
        - name: main
          image: "main:latest"
          imagePullPolicy: "Never"
          command: ["/bin/bash", "-c"]
          args:
            - |
              (until curl -o - ${MYSQL_HOST}:8090/readiness &>/dev/null; do sleep 1; done)
              /main
              curl ${MYSQL_HOST}:8091/quitquitquit
          env:
            - name: MYSQL_HOST
              value: "0.0.0.0"
        - name: cloud-sql-proxy
          image: "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.7.2-alpine"
          args:
            - "--address=0.0.0.0"
            - "--max-sigterm-delay=30s"
            - "--health-check"
            - "--http-port=8090"
            - "--exit-zero-on-sigterm"
            - "--admin-port=8091"
            - "--quitquitquit"
            - "${PROJECT}:${REGION}:${INSTANCE_NAME}"
          ports:
            - name: "mysql-port"
              containerPort: 3306
```

</details>

## 導入された Sidecar Containers

Kubernetes 1.28 で Sidecar Containers がアルファリリースされました.
この機能では initContainer に対して, `restartPolicy: always`を記述することで, 指定されたコンテナ Sidecar コンテナとしてが動作するようになります.
この特殊な initContainer は通常の initContainer とは異なり, 次の性質を持ちます.

- 完了を前提とせず, 終了しなくてもメインコンテナが起動する
  - メインコンテナの実行中も継続して実行される
- readinessProbe, startupProbe が利用できる
  - startupProbe が成功するまでメインコンテナの起動が遅延される

また, Job リソースにおいては, この特殊な initContainer は完了判定の対象コンテナとなりません.

つまり次のようなことが可能となります.

- Sidecar コンテナの startup が完了するまでメインコンテナの起動を待機する
- 特にシグナル送付や共有 Volume 操作なしで Sidecar つきの Job を終了させる

前述の Cloud SQL Proxy の例を書き直すと次のようになります.

```yaml
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "sidecar-test"
spec:
  template:
    spec:
      restartPolicy: "Never"
      containers:
        - name: main
          image: "main:latest"
          imagePullPolicy: "Never"
          command: ["/main"]
          env:
            - name: MYSQL_HOST
              value: "0.0.0.0"
      initContainers:
        - name: cloud-sql-proxy
          image: "gcr.io/cloudsql-docker/gce-proxy:1.33.13"
          restartPolicy: "Always" # Sidecar Container として動作させる
          command:
            - "/cloud_sql_proxy"
            - "-instances=${PROJECT}:${REGION}:${INSTANCE_NAME}=tcp:0.0.0.0:3306"
            - "-term_timeout=30s"
            - "-use_http_health_check"
            - "-health_check_port=8090"
          ports:
            - name: "mysql-port"
              containerPort: 3306
          startupProbe:
            httpGet:
              path: /readiness
              port: 8090
```

図示するとこの通り.

![338baf96770390ab483d4e69.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/594776/21022173-4438-b4b3-8b9d-f4c36327c3d0.png)

超絶シンプルになりました.

```bash
$ kubectl get po -w
NAME                 READY   STATUS     RESTARTS   AGE
sidecar-test-t7f7b   0/2     Init:0/1   0          4s
sidecar-test-t7f7b   0/2     Init:0/1   0          5s
sidecar-test-t7f7b   0/2     PodInitializing   0          11s
sidecar-test-t7f7b   1/2     PodInitializing   0          12s
sidecar-test-t7f7b   2/2     Running           0          13s
sidecar-test-t7f7b   1/2     Completed         0          22s
sidecar-test-t7f7b   0/2     Completed         0          23s
```

### メリット

#### 宣言的に Sidecar コンテナを利用できる

EXIT を trap してファイルを生成したり, ファイルが生成されたかを常に監視して終了処理を起動したり, `curl`で疎通を図ったりするような必要性が皆無になります.
記述は各コンテナで何をしなくてはならないのかに注力でき, シェルスクリプトにバグがないか?みたいな無駄に増えたチェックプロセスも破棄できます.
無論やらなくてはならないことが減る分, 可読性 / メンテナンス性の向上にも繋がります.

#### 不要なシェル系を排除できる

今までのようにシェルスクリプトを書く必要がなくなるので, それに必要だったシェル系やツール群を除去できます.
単純にコンテナサイズの縮小, セキュリティの向上に繋がります.

### デメリット

#### コンテナ起動速度が低下する

initContainer はメインコンテナの起動を遅延するため, その分の待機時間が発生します.
元よりメイン処理が起動する前に待機処理を入れていた場合は問題になりませんが, アプリケーションサイドでカバーしていた場合等は純粋にこの待機時間が起動速度に影響します.
また, 複数の Sidecar Container を利用する場合, initContainer は直列に処理されるため, その分の待機時間が縦伸びします.

## まとめ

- Kubernetes 1.28 でリリースされた Sidecar Containers は特殊な initContainer
- Sidecar Containers はメインコンテナより前から起動し続ける
- Sidecar Containers は Job の完了判定対象とならない
- 今までスクリプト書いてたの何だったん???ってくらいスッキリする

早く GA になってほしいものです.


## お知らせ

DONUTSでは新卒中途問わず積極的に採用活動を行っています.
詳細はこちらをご確認ください.

https://www.donuts.ne.jp/recruit/

ジョブカンのインフラエンジニアも募集中です.

https://recruit.jobcan.jp/donuts/job_offers/1396393


## 参考情報

大本の Issue (2016 年からあります...根が深い)
https://github.com/kubernetes/kubernetes/issues/25908

https://github.com/kubernetes/enhancements/issues/753

https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#api-for-sidecar-containers
