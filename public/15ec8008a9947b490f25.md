---
title: ActiveJob + Pub/Sub の構成をそこそこ真面目に組んだので解説してみる
tags:
  - ActiveJob
  - PubSub
private: false
updated_at: '2024-12-02T07:07:33+09:00'
id: 15ec8008a9947b490f25
organization_url_name: donuts_coltd
slide: false
ignorePublish: false
---
ジョブカン事業部のアドベントカレンダー 2 日目です.

https://qiita.com/advent-calendar/2024/jobcan

ジョブカン採用管理の開発をやってる @Lru です. 去年に引き続き 2 番手での参加です.
私も新・RPG ジョブ診断してみました. どうやら何かを錬成することに長けているようです.

https://rpg-shindan.wantedly.com/result/b6w59/

ということで今年錬成した非同期処理系の解説を書いてみます.

## 背景

ジョブカン採用管理は Ruby on Rails を利用して Web アプリケーションを構築しています.
直近利用企業の規模拡大に伴い, Cloud Pub/Sub を導入し, 時間のかかってしまう処理の並列/非同期実行対応を一部進行しました.

Rails で非同期処理をライブラリとして, Redis を利用する場合は SideKiq, Amazon SQS を利用する場合は Shoryuken といったものが存在しています.
しかし Cloud Pub/Sub を利用するライブラリにて実用に足るものは見当たりませんでした.

ということで今年そこそこ真面目にカスタム実装したので, その解説をしてみる記事です.

## 出来上がったもの

最終的にこのような形態となりました.
(実物はそこそこの長さになってしまうため, エラーレポート処理やヘルスチェック, オプション制御などを一部省略しています)

```ruby:lib/active_job/queue_adapters/pub_sub_adapter.rb
class ActiveJob::QueueAdapters::PubSubAdapter
  require "google/cloud/pubsub"

  SHARED_JOB_ATTRS = ["job_class", "job_id", "queue_name", "enqueued_at"].freeze

  class TopicNotFound < StandardError; end

  def enqueue(job)
    topic = client.find_topic(job.queue_name)
    raise TopicNotFound, "Topic not found: #{job.queue_name}" if topic.nil?

    serialized_data = job.serialize
    topic.publish(serialized_data["arguments"].to_json, **serialized_data.slice(*SHARED_JOB_ATTRS))
  end

  private

    def client
      @client ||= Google::Cloud::PubSub.new(
        project_id: ENV.fetch("GOOGLE_PROJECT_ID"),
        credentials: ENV.fetch("PUBSUB_CREDENTIALS", nil),
      )
    end
end
```

```ruby:lib/pub_sub_worker.rb
class PubSubWorker
  require "google/cloud/pubsub"

  class MessageInvalid < StandardError; end

  class << self
    SHUTDOWN_WAIT = 20

    def run_worker!
      Rails.logger.info("Worker start...")
      subscriber = subscription.listen {|message| subscribe(message) }

      # 復帰不能な理由で失敗した場合
      subscriber.on_error do |e|
        # エラー記録
        # ErrorReporter.report_exception(e)
        # メインスレッドを叩き起こして終了処理を実行させる
        Thread.main.wakeup
      end

      # 非同期処理実行開始
      subscriber.start

      # Graceful Shutdown
      at_exit do
        Rails.logger.info("Worker shutdown...")
        subscriber.stop.wait!(SHUTDOWN_WAIT)
      end

      # 関数を実行状態のまま保つ.
      sleep
    rescue SignalException
      # Ctrl-CやKILL SIGNALを受けた場合は終了する
    end

    def subscribe(message)
      Rails.application.reloader.wrap do
        make_job(message).perform_now
        message.ack!
      rescue JSON::ParserError, MessageInvalid
        # JSONではないデータが送付されてきた場合や, 不当なメッセージが送付されてきた場合再試行しない
        message.ack!
      rescue StandardError
        # ジョブの処理中に予期せぬエラーが発生した場合には再試行する
        message.nack!
      end
    end

    def make_job(message)
      klass = message.attributes["job_class"].safe_constantize
      raise MessageInvalid, "Invalid Job Class: #{message.attributes["job_class"]}" if klass.blank?

      instance = klass.new
      instance.deserialize(message.attributes.merge("arguments" => JSON.parse(message.data)))

      instance
    end

    # --- PubSub処理系
    def client
      @client ||= Google::Cloud::PubSub.new(
        project_id: ENV.fetch("GOOGLE_PROJECT_ID"),
        credentials: ENV.fetch("PUBSUB_CREDENTIALS", nil),
      )
    end

    def topic
      @topic ||= client.topic(ENV.fetch("PUBSUB_TOPIC"))
    end

    def subscription
      @subscription ||= topic.subscription(ENV.fetch("PUBSUB_SUBSCRIPTION"))
    end
  end
end
```

## Message Publish!

### QueueAdapter の実装

ActiveJob で実行される Queue に対して情報を格納する操作は, 指定されている QueueAdapter の`enqueue`メソッドを呼び出すことで行われます.

例えば Rails のデフォルトの指定では `ActiveJob::QueueAdapters::AsyncAdapter`[^1]の`enqueue`メソッドが呼び出されます.
定義の通り, 処理を実行するための ThreadPool が用意されており, そこに情報を受け渡すことで, Rails の Web サーバのリクエスト処理とは別のスレッドでジョブを実行できます.

Cloud Pub/Sub を Queue として利用するためには, カスタムで QueueAdapter を実装する必要があります.
実装としてはとてもシンプルに, Pub/Sub の SDK を利用して, ジョブの情報をメッセージとして発行するだけの処理を行うのみで問題ありません.

```ruby:lib/active_job/queue_adapters/pub_sub_adapter.rb
  def enqueue(job)
    topic = client.find_topic(job.queue_name)
    raise TopicNotFound, "Topic not found: #{job.queue_name}" if topic.nil?

    serialized_data = job.serialize
    topic.publish(serialized_data["arguments"].to_json, **serialized_data.slice(*SHARED_JOB_ATTRS))
  end
```

実装後, Adapter は`ActiveJob::QueueAdapters::PubsubAdapter`として認識されるよう配置します.
このように配置することで, ActiveJob の指定における Adapter 指定の lookup [^2] 対象として認識されるようになります.

```ruby:config/application.rb
config.active_job.queue_adapter = :pub_sub
# こうでもいいけどうーん...
# config.active_job.queue_adapter = ActiveJob::QueueAdapters::PubSubAdapter.new
```

他, QueueAdapter には`enqueue_at`メソッドが用意されており, 必要な場合はこちらにジョブの時刻スケジューリングを実装できます.
Cloud Pub/Sub 自体に時刻指定のスケジューリング機能は存在しないため, 実装する際には Cloud Tasks などの別プロダクトを利用することになるでしょう.
今回の実装では特にスケジューリングの必要はなかったため, この機能を実装しないことを選択しています.

### メッセージの構造

Pub/Sub のメッセージには, データ本体である`data`と, 属性である`attributes`があります. [^3]

|     種別     | 容量                             | フィルタなどの適用 |
| :----------: | :------------------------------ | :----------------- |
|    `data`    | 最大 10MB                        | 適用不可           |
| `attributes` | Key:最大 256B<br>Value: 最大 1KB | 適用可             |

`data`は格納可能なデータ量が多く, 複雑な Job を実行する場合でも十分引数全てを格納できる容量があります.
`attributes`は容量が小さいですが, Subscription 作成時の受信メッセージフィルタの対象に出来るなど, メタデータとしての利用に適しています.

`data`に全ての情報を格納できますが, 今回の実装では`attributes`に Job のメタデータを格納する形を取りました.
今後メタデータを拡充して, 一部の処理は高性能なインスタンスで!とかも実装するかもしれません. 予定は未定です.

メッセージはスリムな状態にしておくとそれだけで運用コストが下がるので, 送付するメタデータを必要なものだけに絞り込んでおくのがオススメです.
現状時刻指定のスケジューリング非対応のため `scheduled_at`などは不要, リトライ回数制御は Dead Letter Topic 処理前提で`executions`なども不要として削っています.

```ruby
      SHARED_ATTRIBUTES = ["job_class", "job_id", "queue_name", "enqueued_at"].freeze

      ...

      def enqueue(job)
        topic = client.find_topic(job.queue_name)
        raise TopicNotFound, "Topic not found: #{job.queue_name}" if topic.nil?

        serialized_data = job.serialize
        topic.publish(serialized_data["arguments"].to_json, **serialized_data.slice(*SHARED_ATTRIBUTES))
      end
```

留意点としては`attributes`には文字列データしか入らないので, `executions`の数値データなどを入れる場合には`subscribe`側で処理実行前に復元処理を挟んでやる必要が生じてきます.

### Serialize

ActiveJob にはジョブのメタデータなど含めてまるごとシリアライズするための`serialize`メソッド[^4]が用意されています.
このメソッドを呼び出すことで, 文字列へと変換が容易な, ジョブの ID や引数などの情報を含むシンプルなハッシュの形式に変換されます.

```ruby
class TestJob < ActiveJob::Base; end

TestJob.new.serialize
# => {
#      "job_class"=>"TestJob",
#      "job_id"=>"6688e2a9-bbfa-45ee-b4f3-773bb9e56110",
#      "provider_job_id"=>nil,
#      "queue_name"=>"default",
#      "priority"=>nil,
#      "arguments"=>[],
#      "executions"=>0,
#      "exception_executions"=>{},
#      "locale"=>"ja",
#      "timezone"=>"Tokyo",
#      "enqueued_at"=>"2024-11-18T09:25:13Z"
#    }
```

このメソッドは Job の引数のシリアライズ処理に対応しています.
例えば ActiveRecord のインスタンスは, ジョブを実行する際 復元可能なよう DB 上に保存されたデータへのリンクとしてシリアライズされます.

```ruby
# serializeを利用する場合
TestJob.new(data: Address.first).serialize["arguments"].to_json
# => "[{\"data\":{\"_aj_globalid\":\"gid://webapp/Address/1\"},\"_aj_ruby2_keywords\":[\"data\"]}]"

# to_jsonする場合
> TestJob.new(data: Address.first).arguments.to_json
# => "[{\"data\":{\"id\":1,\"prefecture\":\"東京都\",\"postcode\":\"151-0053\",\"city\":\"渋谷区\",\"street\":\"代々木2丁目2-1\",\"building\":\"小田急サザンタワー8F\", ...}}}]"
```

これを利用することで, Queue に蓄積されるデータ量の削減とともに, ジョブ内でのデータの復元処理を簡略化できます.
また, ActiveJob 公式のドキュメントに従った Serializer 実装による, データのシリアライズ/デシリアライズに対応可能な範囲を拡張可能な挙動も維持されます.

https://railsguides.jp/active_job_basics.html#%E3%82%B7%E3%83%AA%E3%82%A2%E3%83%A9%E3%82%A4%E3%82%B6

## Message Subscribe!

### Subscribe 方式

Cloud Pub/Sub を利用して, メッセージを受信する方法はざっくり 2 通りあります. [^5]

| 種別 | 説明                                                                         |
| :--: | :--------------------------------------------------------------------------- |
| Pull | メッセージを処理する側が取得しに行く                                         |
| Push | メッセージを受信するエンドポイントを指定し, そこにリクエストを送付してもらう |

今回対応したかったのは「時間のかかってしまう処理を裏側で実行したい」という課題でした.
実装は可能な限りリアルタイムに処理を開始でき, 並列性の確保も容易な Pull 方式の中の Streaming Pull 式を採用しました.

Cloud Pub/Sub の SDK はなかなか使い勝手が良く, `listen`メソッドに対して処理する内容をブロックで渡して`start`するだけで, 次のような多くの処理を行ってくれます.

- 処理実行スレッド管理
- ストリーミング接続の確保
- 処理時の Ack タイムアウトの延長処理

今回コードでは割愛していますが, 並列数の制御や, 同時に処理待ち状態にしておけるメッセージ数上限なども柔軟に設定が可能です.

```ruby
      subscriber = subscription.listen(threads: { callback: ENV.fetch("RAILS_MAX_THREADS", 5) }) {|message| subscribe(message) }
      ...
      subscriber.start
```

### Job の Worker 上での実行

ジョブの実行は再構築したジョブの`perform_now`メソッドを呼び出すことで行っています.
他にもジョブの即時実行メソッドは存在しており, 比較すると次の差異があります.

| メソッド名  | ドキュメント記載 | `xxxxx_perform` callback | 定数の自動再読込 |
| :---------: | :--------------- | :----------------------- | :----------------- |
|   perform   | あり             | 実行されない             | 実行されない       |
| perform_now | あり             | 実行される               | 実行されない       |
|   execute   | なし             | 実行される               | 実行される         |

`perform`を呼び出せば最低限ジョブの実行は行われますが, `before_perform`などのコールバックの実行は含まれず無視されます.
`execute`はドキュメントに記載のないメソッドで, `around_execute`の callback によるジョブ実行周辺でのアプリケーションコードのオートリロード処理が搭載されています.

https://github.com/rails/rails/blob/v7.2.2/activejob/lib/active_job/execution.rb#L26-L31

対応範囲から見れば`execute`が最も適しているように見えますが, ドキュメント記載がないこと, また Callback が非自明的に定義されていることなどから, 利用しない方針としました.
カスタム実装している都合上, 今後も改修していく可能性があり, その際にあまり認識負荷は高めたくないためです.
ActiveJob のメソッドとして名高い `perform_now` のほうが, チームの開発者にも認識しやすいだろうということで, こちらを採用しています.

```ruby
    def subscribe(message)
      ...
        make_job(message).perform_now
        message.ack!
      ...
```

ライブラリなど, その機能を専門に取り扱う者のみが開発する場合は, `execute`を利用するのも良いでしょう.

```ruby
    ...
    def subscribe(message)
      ActiveJob::Base.execute(message.attributes.merge("arguments" => JSON.parse(message.data)))
      message.ack!
    ...

```

### Job のオートリロード

Rails 管轄の QueueAdapter を利用している場合は, `execute`メソッドを利用しているため, ActiveJob はアプリケーションコードをオートリロードする機能を持っています.
カスタム QueueAdapter でも同じようにすれば, オートリロード機能を利用できますが, 前述の理由から利用しないことを選択しています.

開発中においてはオートリロードは非常に有用な機能であり, 是非搭載しておきたいものです.
ということで前述の`execute`メソッドで実行されるオートリロード処理相当の記述を Worker に追加しています.

```ruby
      Rails.application.reloader.wrap do
        make_job(message).perform_now
        message.ack!
      ...
```

この記述により, `wrap`の内部のブロックはアプリケーションコードの変更があった際に, 安全なタイミングで定数のリロードが実行された上で実行されるようになります.
詳細は [Rails のスレッドとコード実行](https://railsguides.jp/threading_and_code_execution.html#reloader) などを参照してください.

なお Ruby における「定数」は固定値のみを指すものではなく, クラスやモジュールのことも含みます. クラスやモジュール定義の再読み込み ∈ 定数の再読み込みです.

https://docs.ruby-lang.org/ja/latest/doc/spec=2fvariables.html#const

### Deserialize

ActiveJob の`serialize`メソッドを利用した場合は, その逆操作を担う`deserialize`メソッド[^6]を利用する形になります.

`ActiveJob::Core.deserialize`も利用できますが, こちらは存在しないジョブが指定されると定数が見つからない状態となりそのままエラーが発生します.
メッセージが不当だった場合のエラーは特殊エラーとして切り分けるべく, ジョブの存在確認を行った上でデシリアライズ処理を行うようにしています.

```ruby
    def make_job(message)
      klass = message.attributes["job_class"].safe_constantize
      raise MessageInvalid, "Invalid Job Class: #{message.attributes["job_class"]}" if klass.blank?

      instance = klass.new
      instance.deserialize(message.attributes.merge("arguments" => JSON.parse(message.data)))

      instance
    end
```

なお `deserialize` メソッドは, 呼び出し時点では引数にあたる`arguments`をデシリアライズしません.
そして引数のデシリアライズ処理は, `perform_now`または`execute`が呼び出された際に初めて実行され, `perform`では実行されません.
この点も`perform_now`メソッドを呼び出す方針を取った理由の 1 つです.

実行前に引数のデシリアライズ処理を行いたい場合は, 明示的にデシリアライズ処理を行うこともできます.

```ruby
    instance.arguments = ActiveJob::Arguments.deserialize(JSON.parse(message.data))
```

## まとめ

カスタムQueueAdapterの実装とその内容の解説をやってみました.
あまり機能を盛ってはいませんが, Rails と Cloud Pub/Sub を利用する方々のなにか参考になっていたら幸いです.

ActiveJob をなんとなく使う分にはあまり意識しなくても良い部分が多く, かなり便利なのですが, こういったカスタム実装をしようとすると考慮すべき点が多いというかドキュメントもっと欲しいなー感じた次第でした.
まぁ Rails の裏側見学したような感じで中々面白かったです.

## 終わりに

今回この Adapter を使って今まで同期処理だったものを非同期処理に置き換える, そこそこの大工事をしたわけなのですが, 改善可能な点をアーキテクチャレベルから見直していくような, こうした活動を実行に移していけるのが弊ジョブカン採用管理チームの良いところだと思っています.

そんな弊チームの属するジョブカン事業部では現在積極的に採用活動を行っています.
もし興味を持っていただけた方, 挑戦するのが得意な錬金術師の方, いらっしゃいましたら, ぜひ検討してみてください.

https://www.donuts.ne.jp/recruit/

https://recruit.jobcan.jp/donuts/show/B001/1396297

### おまけ

今回の経験を通じて ActiveJob 周りの知見がそこそこ溜まったので, 勢いとノリで同じようなことをほぼやってくれるライブラリを個人で作ってみました.
こちらはこちらで, Rails の Generator や Rake タスクの自動登録などそこそこ知見があったため, また別の機会に紹介したいです.

https://rubygems.org/gems/pubilion

それでは.

#### 参考文献

先行研究.

https://tech.itandi.co.jp/entry/2017/05/rails-active-job-gcp-pubsub/

[^1]: [AsyncAdapter](https://github.com/rails/rails/blob/v7.2.2/activejob/lib/active_job/queue_adapters/async_adapter.rb)
[^2]: [Adapter Lookup](https://api.rubyonrails.org/v7.2.2/classes/ActiveJob/QueueAdapters.html#method-c-lookup)
[^3]: [Message の形式](https://cloud.google.com/pubsub/docs/publish-message-overview?hl=ja#about_messages)
[^4]: [ActiveJob Serialize](https://api.rubyonrails.org/v7.2.2/classes/ActiveJob/Core.html#method-i-serialize)
[^5]: [Subscription 比較表](https://cloud.google.com/pubsub/docs/subscriber?hl=ja#subscription_type_comparison)
[^6]: [ActiveJob Deserialize](https://api.rubyonrails.org/v7.2.2/classes/ActiveJob/Core.html#method-i-deserialize)
