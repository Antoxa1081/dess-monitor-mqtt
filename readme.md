Example [config.yaml](.example.config.yaml)

Copy `.example.config.yaml` and rename to `config.yaml`
```yaml

username: "YOUR_USERNAME"
password_hash: "YOUR_PASSWORD_SHA1_HASH" # or you can use plain password field "password"
password: "YOUR_PLAIN_PASSWORD"

devices:
  - pn: "Q00000000001" # set your target pn
#  - pn: "Q00000000002" # you can set many targets, or one

mqtt:
  host: "localhost"
  port: 1883
  user: ""
  pass: ""
  base_topic: "home/inverters"

poll_interval: 60  # seconds, smartess updates cloud data with default 5 minutes interval
```

Set settings PUB topic:

Topic: `home/inverters/Q00000000001/ctrl_fields/param_value/set`

send string value `param_value`

param_id - from `home/inverters/Q00000000001/ctrl_fields/*`

param_value - from `home/inverters/Q00000000001/ctrl_fields/{param_id}/options` → `{ "key" }`

```
mosquitto_pub -h localhost -t "home/inverters/Q00000000001/ctrl_fields/bat_ac_charging_current/set" -m "020"
```
```
mosquitto_sub -h localhost -t "home/inverters/Q00000000001/ctrl_fields/bat_ac_charging_current/set_status" -v
```

*`bat_ac_charging_current` for each inverter this param name may be various