const form = document.querySelector("#login-form");
const password = document.querySelector("#password");
const toggle = document.querySelector("#toggle-password");
const submit = document.querySelector("#login-submit");
const status = document.querySelector("#login-status");

// 登录接口接入前只演示表单交互，不读取、发送或保存账号密码。
form.addEventListener("submit", (event) => {
  event.preventDefault();
  status.textContent = "登录服务尚未接入，可先预览工作台。";
});

toggle.addEventListener("click", () => {
  const visible = password.type === "password";
  password.type = visible ? "text" : "password";
  toggle.setAttribute("aria-pressed", String(visible));
  toggle.setAttribute("aria-label", visible ? "隐藏密码" : "显示密码");
  toggle.textContent = visible ? "隐藏" : "显示";
});

// 先安装提交拦截，再启用控件；脚本未加载时表单保持不可提交。
toggle.disabled = false;
submit.disabled = false;
