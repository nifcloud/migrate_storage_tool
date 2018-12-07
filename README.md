# Migrate Object Storage Tool

S3互換API対応ストレージ間でのデータ移行用ツール

## 動作環境
* 下記環境で動作確認しています。
    - ruby: 2.1.0 
    - AWS SDK for Ruby(v1)

## インストール
* git clone
```
# git clone https://github.com/nifcloud/migrate_storage_tool.git
```
* mkdir
```
# cd migrate_storage_tool
# bundle install --path vendor/bundle
# mkdir tmp log
```

## 設定ファイル
* config.ymlがあるので、それぞれ設定してください。
    - multiple: 多重数(デフォルトはcpu数が1、スレッド数が3)
    - multipart: マルチマートアップロードを使用する際の条件
        - gigabyte_size_to_be_mpu: このサイズ以上のファイルであればマルチパートアップロードする(GB)
        - part_megabyte_size: partあたりのサイズ(MB)
    - memory: メモリ管理(デフォルトのガベージコレクション実行頻度は100)
    - src: 移行元情報 
    - dst: 移行先情報

## 実行
* 実行コマンド
```
# bundle exec ruby copy_storage_to_another.rb
```
※移行対象のオブジェクトリスト(listfile)が、並列処理ごとに`./tmp`に出力されます。


## 再開
* 再開コマンド
    - 指定されたlistfileの先頭から移行を再開します。
    - 実施済みのオブジェクトを消すなど、適宜listfileを編集してから使用してください。
```
# bundle exec ruby continue_storage_to_another_from_file.rb <listfile>
```

## 補足
* ACLは以下のものだけ対応しています。(ニフクラコンパネのエクスプローラから設定できるものだけ)
    - :public_read
    - :authenticated_read
    - :private
        - 処理が重いのでACL対応は現在無効化してあります。
