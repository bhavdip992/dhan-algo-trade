#!/bin/bash
# setup_ec2.sh — Run once on a fresh Amazon Linux 2 / Ubuntu EC2 instance
# Usage: bash setup_ec2.sh

set -e

APP_DIR="$HOME/algo-trade-dhan"
VENV="$APP_DIR/venv"
SERVICE_NAME="dhan-algo"

echo "=== [1/5] Installing system packages ==="
if command -v apt-get &>/dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-pip python3-venv git
else
    sudo yum update -y
    sudo yum install -y python3 python3-pip git
fi

echo "=== [2/5] Creating virtualenv ==="
python3 -m venv "$VENV"
"$VENV/bin/pip" install --upgrade pip
"$VENV/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "=== [3/5] Setting .env permissions ==="
chmod 600 "$APP_DIR/.env"

echo "=== [4/5] Installing systemd service ==="
# Replace User= with actual current user
sed "s/User=ec2-user/User=$(whoami)/" "$APP_DIR/dhan-algo.service" \
    | sudo tee /etc/systemd/system/$SERVICE_NAME.service > /dev/null

sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME

echo "=== [5/5] Done ==="
echo ""
echo "Commands:"
echo "  sudo systemctl start   $SERVICE_NAME   # start algo"
echo "  sudo systemctl stop    $SERVICE_NAME   # stop algo"
echo "  sudo systemctl restart $SERVICE_NAME   # restart"
echo "  sudo systemctl status  $SERVICE_NAME   # check status"
echo "  tail -f $APP_DIR/dhan_live.log         # live logs"
