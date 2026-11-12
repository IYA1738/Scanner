#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf '%s\n' "$*" >&2
    exit 1
}

(( EUID == 0 )) || fail '请使用 sudo bash frontend/deploy.sh 执行部署。'

frontend_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
dist="$frontend_dir/dist"
target=/var/www/scanner
site=/etc/nginx/sites-available/scanner
enabled=/etc/nginx/sites-enabled/scanner
default_site=/etc/nginx/sites-available/default
default_link=/etc/nginx/sites-enabled/default

for page in index.html workspace.html modules/scanner/index.html; do
    [[ -f "$dist/$page" ]] || fail "缺少构建文件：$dist/$page，请先运行 npm run build。"
done
[[ -d "$dist/assets" ]] || fail '构建文件缺少 assets 目录。'
[[ -f "$site" ]] || fail "缺少站点配置：$site"
[[ ! -L "$target" ]] || fail "部署目录是软链接，请先核对：$target"

# 只切换已知站点，遇到其他配置时保留现场。
for link in "$enabled" "$default_link"; do
    if [[ -e "$link" || -L "$link" ]]; then
        [[ -L "$link" ]] || fail "已有同名普通文件，停止部署：$link"
        expected="$site"
        [[ "$link" == "$default_link" ]] && expected="$default_site"
        [[ "$(readlink -f -- "$link")" == "$expected" ]] || fail "软链接指向其他站点，停止部署：$link"
    fi
done
[[ -z "$(find "$dist" ! -type d ! -type f -print -quit)" ]] || fail '构建目录含有非常规文件，停止部署。'
if [[ -d "$target" ]]; then
    [[ -z "$(find "$target" ! -type d ! -type f -print -quit)" ]] || fail '部署目录含有非常规文件，停止部署。'
fi

# 只复制构建产物；保留已有哈希资源，避免旧页面加载资源时出现 404。
install -d -m 0755 "$target"
cp -R -- "$dist/." "$target/"
find "$target" -type d -exec chmod 0755 {} +
find "$target" -type f -exec chmod 0644 {} +

created_link=0
removed_default=0
restore_links() {
    set +e
    (( created_link )) && unlink "$enabled"
    (( removed_default )) && ln -s "$default_site" "$default_link"
    printf '%s\n' '启用失败，站点链接已恢复；请检查上面的错误。' >&2
}
trap 'restore_links; exit 1' ERR

if [[ ! -L "$enabled" ]]; then
    ln -s "$site" "$enabled"
    created_link=1
fi
if [[ -L "$default_link" ]]; then
    unlink "$default_link"
    removed_default=1
fi

nginx -t
systemctl reload nginx.service
trap - ERR

printf '%s\n' '前端部署完成：登录 /，工作台 /workspace.html，扫链模块 /modules/scanner/'
