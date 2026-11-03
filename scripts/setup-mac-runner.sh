#!/bin/bash
# One-shot setup of the korail-home self-hosted runner on a Mac.
# Usage (in Terminal):
#   bash <(curl -fsSL https://raw.githubusercontent.com/chang0580/ktx-srt-watcher/main/scripts/setup-mac-runner.sh)
# Follows docs/SELF_HOSTED_RUNNER.md: Homebrew python (>=3.11), runner with the
# korail-home label, LaunchAgent service, PATH .env fix, and no sleep on AC.
set -euo pipefail

REPO="${REPO:-chang0580/ktx-srt-watcher}"
RUNNER_DIR="$HOME/actions-runner"
say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
ask() { local a; read -r -p "$1" a </dev/tty; printf '%s' "$a"; }

[ "$(uname -s)" = Darwin ] || { echo "macOS 전용 스크립트예요."; exit 1; }
case "$(uname -m)" in
  arm64) ARCH=arm64; BREW_PREFIX=/opt/homebrew ;;
  *)     ARCH=x64;   BREW_PREFIX=/usr/local ;;
esac
say "Mac 확인: $ARCH"

say "1/5 Homebrew + Python"
if [ ! -x "$BREW_PREFIX/bin/brew" ]; then
  echo "Homebrew를 설치해요. Mac 로그인 비밀번호를 물으면 입력하세요 (화면에 안 보여요)."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" </dev/tty
fi
eval "$("$BREW_PREFIX/bin/brew" shellenv)"
brew install python git >/dev/null
PY="$BREW_PREFIX/bin/python3"
"$PY" -c 'import sys; assert sys.version_info >= (3, 11), sys.version' \
  || { echo "Python 3.11 이상이 필요해요: $("$PY" --version)"; exit 1; }
echo "$("$PY" --version) OK"

say "2/5 GitHub 러너 내려받기"
mkdir -p "$RUNNER_DIR" && cd "$RUNNER_DIR"
if [ ! -f config.sh ]; then
  # sed reads to EOF: an early-exiting filter (grep -m1, head) SIGPIPEs curl
  # and pipefail aborts the script silently.
  ver=$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest \
        | sed -nE 's/.*"tag_name": *"v([^"]+)".*/\1/p')
  [ -n "$ver" ] || { echo "러너 최신 버전을 못 찾았어요. 잠시 뒤 다시 실행하세요."; exit 1; }
  echo "runner v$ver (osx-$ARCH)"
  curl -fsSL -o runner.tar.gz \
    "https://github.com/actions/runner/releases/download/v${ver}/actions-runner-osx-${ARCH}-${ver}.tar.gz"
  tar xzf runner.tar.gz && rm runner.tar.gz
fi

say "3/5 러너 등록 (korail-home)"
if [ ! -f .runner ]; then
  echo "브라우저에서 아래 페이지를 열고, Configure 칸의 '--token' 뒤 값만 복사해 붙여 넣으세요:"
  echo "  https://github.com/$REPO/settings/actions/runners/new?arch=$ARCH&os=osx"
  TOKEN=$(ask "등록 토큰: ")
  [ -n "$TOKEN" ] || { echo "토큰이 비었어요."; exit 1; }
  ./config.sh --unattended --replace --url "https://github.com/$REPO" --token "$TOKEN" \
    --name "$(scutil --get LocalHostName 2>/dev/null || hostname -s)-korail" --labels korail-home
else
  echo "이미 등록돼 있어요 — 건너뜀"
fi

say "4/5 서비스 등록 (로그인하면 자동 시작)"
grep -q '^PATH=' .env 2>/dev/null \
  || echo "PATH=$BREW_PREFIX/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" >> .env
./svc.sh install >/dev/null 2>&1 || true
./svc.sh stop >/dev/null 2>&1 || true
./svc.sh start
./svc.sh status | grep -iE 'started|running|pid' || true

say "5/5 잠자기 방지 (전원 연결 시 시스템은 깨어 있고 화면만 10분 뒤 꺼짐)"
if [ "$(ask '설정할까요? Mac 비밀번호가 필요해요 [Y/n]: ')" != "n" ]; then
  sudo pmset -c sleep 0 displaysleep 10 </dev/tty
  echo "OK — 덮개는 열어 두고 전원 어댑터를 꽂아 두세요."
fi

printf '\n\033[1;32m완료! https://github.com/%s/settings/actions/runners 에서 초록 Idle이면 성공이에요.\033[0m\n' "$REPO"
