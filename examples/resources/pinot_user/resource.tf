# Pinot user example
resource "pinot_user" "user_broker" {
  username    = "user"
  component   = "BROKER"
  role        = "USER"
  permissions = ["READ"]
  password    = "password"
}
