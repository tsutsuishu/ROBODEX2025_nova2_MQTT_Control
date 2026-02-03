# ROBODEX2025_nova2_MQTT_Control
MQTT Control for nova2

requirements.txtはcobottaを参考に作成した

- src/nova2/config：関節可動限界を設定
- src/nova2/nova2_control：関節，EE，enableの制御などロボット全体の制御プロセス
- src/nova2/nova2_monitor：フィードバック取得
- src/nova2/nova2_mqtt_control：MQTTのサブスクライブ，プロセス間通信の定義
- src/nova2/nova2_robot：nova2と直接通信する処理
- src/main.py：GUI関連(他プロセスの起動はGUIから)