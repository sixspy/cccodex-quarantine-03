import { api } from '../api.js';

function shortFp(fp) {
  return fp ? fp.replace('sha256:', '').slice(0, 10) : '';
}

export async function renderRework(el) {
  const data = await api('/api/rework/jobs');
  el.innerHTML = `
    <div class="card">
      <h2>回炉队列</h2>
      <p class="muted">开跑当刻由服务端一次落定钉点（物化包封存条目键与原因码），钉点只读、拒再改；同一回炉单禁止双开。</p>
      <table>
        <thead><tr><th>任务</th><th>条目（钉点换源）</th><th>舱</th><th>状态</th><th>次数</th><th>钉点出处</th><th>操作</th></tr></thead>
        <tbody>
          ${(data.items || []).map((j) => `
            <tr>
              <td>${j.job_code}</td>
              <td>
                ${j.pin_source ? `
                  <div><b>${j.item_key}</b> <span class="muted">（钉点封存）</span></div>
                  <div class="muted">原因码：${j.pin_reason_code || j.reason}</div>
                  <div class="muted">指纹：${shortFp(j.pin_fingerprint)} · ${j.pinned_at || ''}</div>
                ` : `
                  <div>${j.item_key}</div>
                  <div class="muted">原因：${j.reason}</div>
                `}
              </td>
              <td>${j.bin_code}</td>
              <td><span class="chip">${j.status}</span></td>
              <td>${j.attempt}</td>
              <td>${j.pin_source
                ? `<span class="chip pin-badge">钉点·${j.pin_source}</span>`
                : '<span class="muted">未钉点</span>'}</td>
              <td class="row">
                ${['排队', '失败'].includes(j.status) ? `
                  <button data-ok="${j.id}">开跑成功</button>
                  <button data-bad="${j.id}" class="ghost">开跑失败</button>
                ` : (j.message || '')}
              </td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
  const run = async (id, succeed) => {
    try {
      await api(`/api/rework/jobs/${id}/run`, {
        method: 'POST', body: JSON.stringify({ succeed }),
      });
    } catch (e) {
      alert(e.message || '开跑失败，请重试');
    }
    await renderRework(el);
  };
  el.querySelectorAll('[data-ok]').forEach((btn) => {
    btn.onclick = () => run(btn.dataset.ok, true);
  });
  el.querySelectorAll('[data-bad]').forEach((btn) => {
    btn.onclick = () => run(btn.dataset.bad, false);
  });
}
