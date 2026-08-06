if (!window.Vue) {
  const root = document.getElementById('app')
  if (root) {
    root.innerHTML = '<div style="margin:24px;padding:16px;border:1px solid #f1b3b3;background:#fff2f2;color:#8a1f1f;border-radius:8px;">前端依赖加载失败：Vue 未成功加载。请检查网络或刷新页面后重试。</div>'
  }
} else {
const { createApp, reactive } = Vue

function apiFetch(path, options = {}, apiKey) {
  const headers = options.headers || {}
  if (apiKey) headers['X-API-KEY'] = apiKey
  headers['Content-Type'] = 'application/json'
  return fetch(path, { ...options, headers })
}

createApp({
  data() {
    return {
      apiKey: '',
      health: '-',
      updateInfo: null,
      updateStatus: { status: 'idle', running: false, message: '' },
      updateChecking: false,
      updateApplying: false,
      tasks: [],
      runs: [],
      taskGroups: [],
      tasksPage: { items: [], total: 0, page: 1, page_size: 20 },
      runsPage: { items: [], total: 0, page: 1, page_size: 20 },
      selectedTaskIds: [],
      selectedRunIds: [],
      downloadPreview: { matched_runs: 0, matched_files: 0 },
      runsFilterTaskId: null,
      tasksQuery: { q: '', task_kind: 'all', task_group: '', sort_by: 'id', sort_order: 'desc' },
      runsQuery: { status: '', sort_by: 'started_at', sort_order: 'desc', month: '', data_month: '', task_kind: 'all', file_format: 'csv' },
      tasksPageJump: 1,
      runsPageJump: 1,
      dataSourceCatalog: { nfa: ['default'], edc: [] },
      ui: { showSourceAdmin: false, showBulkImport: false },
      groupAdmin: { new_name: '', selected_name: '', rename_to: '' },
      groupAssignSavingTaskId: null,
      notice: { message: '', kind: 'info', at: 0 },
      sourceAdmin: {
        source_type: 'all',
        items: [],
        selected_instance: '',
        selected_source_type: '',
        new_instance: '',
        editor_text: '{}',
        test_result: null,
        audit_items: [],
        audit_limit: 50,
        rotate_old_seed: '',
        rotate_new_seed: '',
        rotate_policy: { enabled: true, interval_days: 30, last_rotated_at: '' }
      },
      bulkImportText: '',
      bulkImportItems: [],
      bulkPrecheckSummary: { total: 0, conflict_items: 0, name_conflict: 0, key_conflict: 0, batch_name_conflict: 0, batch_key_conflict: 0 },
      bulkDefaults: {
        data_source_instance: 'ali',
        active: true,
        kind: 'periodic',
        schedule_type: 'cron',
        schedule_expr: '0 1 1 * *',
        schedule_time_of_day: '',
        timezone: 'Asia/Shanghai',
        window_selector: 'last_month',
        window_n_days: 30,
        settlement_mode: 'daily_95_avg',
        unit_base: 1024,
        export_formats: ['csv'],
        output_filename_template: '',
        data_budget_enabled: true,
        data_budget_mul: 8,
        data_budget_div: 300
      },
      newTask: {
        name: '',
        group_name: '',
        active: true,
        kind: 'one_off',
        data_source_type: 'nfa',
        data_source_instance: 'default',
        schedule_type: null,
        schedule_expr: null,
        schedule_time_of_day: null,
        timezone: 'Asia/Shanghai',
        window_selector: 'custom',
        window_params: {},
        params: { direction: 'both', export_daily: false, sortby: '', sort_order: 'desc', aggregate_all: false, combine_v4_v6: false, merge_key: '', batch_size: 200, school: '', exclude_school: '', unit_base: 1024, settlement_mode: 'range_95', monthly_aggregate: false, data_budget_enabled: false, data_budget_mul: 8, data_budget_div: 300, edc_name: '', edc_match_mode: 'prefix' },
        export_formats: ['csv'],
        output_filename_template: ''
      },
      editTask: null,
      logViewer: { visible: false, jobId: '', content: '', loading: false, error: '' },
      matchViewer: { visible: false, loading: false, error: '', data: null }
    }
  },
  methods: {
    async extractApiError(res, fallback = '') {
      const fallbackMsg = fallback || `请求失败(${res.status})`
      if (!res) return fallbackMsg
      let data = null
      let text = ''
      try { data = await res.clone().json() } catch (_) {}
      if (!data) {
        try { text = String(await res.clone().text() || '').trim() } catch (_) {}
      }
      let msg = ''
      if (data && typeof data === 'object') {
        if (typeof data.detail === 'string') msg = data.detail
        else if (data.detail && typeof data.detail === 'object') msg = String(data.detail.message || data.detail.detail || data.detail.code || '').trim()
        if (!msg && typeof data.message === 'string') msg = data.message
        if (!msg && typeof data.error === 'string') msg = data.error
      }
      if (!msg && text) {
        msg = text
        try {
          const parsed = JSON.parse(text)
          if (parsed && typeof parsed === 'object') {
            if (typeof parsed.detail === 'string') msg = parsed.detail
            else if (parsed.detail && typeof parsed.detail === 'object') msg = String(parsed.detail.message || parsed.detail.detail || parsed.detail.code || '').trim()
            else if (typeof parsed.message === 'string') msg = parsed.message
          }
        } catch (_) {}
      }
      if (!msg) msg = fallbackMsg
      if (/Task name already exists/i.test(msg) || /UNIQUE constraint failed: tasks\.name/i.test(msg)) {
        msg = '任务名称已存在，请更换后重试'
      }
      return msg
    },
    async showApiError(action, res, fallback = '') {
      const msg = await this.extractApiError(res, fallback)
      alert(`${action}失败：${msg}`)
      return msg
    },
    notify(message, kind = 'info', durationMs = 3800) {
      const msg = String(message || '').trim()
      if (!msg) return
      const now = Date.now()
      const lastMsg = String((this.notice && this.notice.message) || '')
      const lastAt = Number((this.notice && this.notice.at) || 0)
      if (lastMsg === msg && (now - lastAt) < 3000) return
      this.notice = { message: msg, kind: kind || 'info', at: now }
      if (this._noticeTimer) clearTimeout(this._noticeTimer)
      if (durationMs > 0) {
        this._noticeTimer = setTimeout(() => {
          if ((this.notice && this.notice.message) === msg) {
            this.notice = { message: '', kind: 'info', at: 0 }
          }
        }, durationMs)
      }
    },
    runEndReason(run) {
      if (!run || typeof run !== 'object') return ''
      if (run.error_message) return String(run.error_message)
      if (Array.isArray(run.artifacts)) {
        const marked = run.artifacts.find((a) => a && typeof a === 'object' && a.terminal_reason)
        if (marked && marked.terminal_reason) return String(marked.terminal_reason)
      }
      return ''
    },
    runTerminalCode(run) {
      if (!run || typeof run !== 'object') return ''
      if (Array.isArray(run.artifacts)) {
        const marked = run.artifacts.find((a) => a && typeof a === 'object' && a.terminal_code)
        if (marked && marked.terminal_code) return String(marked.terminal_code).toUpperCase()
      }
      return ''
    },
    runEndReasonText(run) {
      const reason = this.runEndReason(run)
      if (!reason) return ''
      const code = this.runTerminalCode(run)
      if (code === 'QUERY_TIMEOUT' || code === 'QUERY_FAILED') {
        return `查询失败（超时或数据库异常）：${reason}`
      }
      if (code === 'EMPTY_RESULT') {
        return `确认为空数据：${reason}`
      }
      return reason
    },
    // 将 'YYYY-MM-DD HH:MM:SS' 转换为 <input type=datetime-local> 需要的 'YYYY-MM-DDTHH:MM:SS'
    toLocalInputString(str) {
      if (!str || typeof str !== 'string') return ''
      return str.replace(' ', 'T')
    },
    // 将 datetime-local 值转换为后端需要的 'YYYY-MM-DD HH:MM:SS'
    fromLocalInputString(str) {
      if (!str || typeof str !== 'string') return ''
      return str.replace('T', ' ')
    },
    // 规范化自定义时间窗口：把日期选择(start_date/end_date)同步到 start_time/end_time，
    // 并在编辑时把已有值回填为日期（固定开始 00:00:00，结束 23:59:59）
    normalizeCustomWindow(obj) {
      if (!obj) return
      const wp = obj.window_params = (obj.window_params || {})
      if (obj.window_selector !== 'custom') return
      // 若用户选择了日期，生成标准时间串
      if (wp.start_date) {
        wp.start_time = `${wp.start_date} 00:00:00`
      }
      if (wp.end_date) {
        wp.end_time = `${wp.end_date} 23:59:59`
      }
      // 若处于编辑状态，只存在 start_time/end_time，则回填日期字段
      if (!wp.start_date && wp.start_time) {
        const s = String(wp.start_time)
        wp.start_date = s.split(' ')[0]
      }
      if (!wp.end_date && wp.end_time) {
        const e = String(wp.end_time)
        wp.end_date = e.split(' ')[0]
      }
    },
    fmtYMD(d) {
      const p = (x) => String(x).padStart(2, '0')
      return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())}`
    },
    initRangePickerNew() {
      if (typeof window.flatpickr !== 'function') return
      const el = document.getElementById('range-new')
      if (!el) return
      if (this._fpNew) { try { this._fpNew.destroy() } catch {} this._fpNew = null }
      const obj = this.newTask
      this.normalizeCustomWindow(obj)
      const def = []
      if (obj.window_params.start_date) def.push(obj.window_params.start_date)
      if (obj.window_params.end_date) def.push(obj.window_params.end_date)
      this._fpNew = flatpickr(el, {
        mode: 'range', locale: 'zh', dateFormat: 'Y-m-d', defaultDate: def,
        onChange: (sel) => {
          if (!sel || !sel.length) return
          const s = this.fmtYMD(sel[0])
          const e = this.fmtYMD(sel[sel.length > 1 ? 1 : 0])
          this.newTask.window_params.start_date = s
          this.newTask.window_params.end_date = e
          this.normalizeCustomWindow(this.newTask)
        }
      })
    },
    initRangePickerEdit() {
      if (typeof window.flatpickr !== 'function') return
      const el = document.getElementById('range-edit')
      if (!el) return
      if (this._fpEdit) { try { this._fpEdit.destroy() } catch {} this._fpEdit = null }
      if (!this.editTask) return
      const obj = this.editTask
      this.normalizeCustomWindow(obj)
      const def = []
      if (obj.window_params.start_date) def.push(obj.window_params.start_date)
      if (obj.window_params.end_date) def.push(obj.window_params.end_date)
      this._fpEdit = flatpickr(el, {
        mode: 'range', locale: 'zh', dateFormat: 'Y-m-d', defaultDate: def,
        onChange: (sel) => {
          if (!sel || !sel.length) return
          const s = this.fmtYMD(sel[0])
          const e = this.fmtYMD(sel[sel.length > 1 ? 1 : 0])
          this.editTask.window_params.start_date = s
          this.editTask.window_params.end_date = e
          this.normalizeCustomWindow(this.editTask)
        }
      })
    },
    async checkHealth() {
      const res = await fetch('/api/health')
      const data = await res.json()
      this.health = data.status
    },
    async checkUpdate() {
      this.updateChecking = true
      try {
        const res = await apiFetch('/api/meta/update', {}, this.apiKey)
        if (!res.ok) {
          const msg = await this.extractApiError(res, '检查更新失败')
          this.updateInfo = { ok: false, message: msg }
          return
        }
        this.updateInfo = await res.json()
      } catch (e) {
        this.updateInfo = { ok: false, message: String(e) }
      } finally {
        this.updateChecking = false
      }
      await this.refreshUpdateStatus(true)
    },
    async refreshUpdateStatus(silent = false) {
      try {
        const res = await apiFetch('/api/meta/update/status', {}, this.apiKey)
        if (!res.ok) {
          if (!silent) this.notify(await this.extractApiError(res, '获取升级状态失败'), 'warning')
          return null
        }
        const data = await res.json()
        this.updateStatus = data || { status: 'unknown', running: false }
        if (this.updateStatus && this.updateStatus.running) {
          this.startUpdateStatusPolling()
        } else {
          this.stopUpdateStatusPolling()
        }
        return this.updateStatus
      } catch (e) {
        if (!silent) this.notify('获取升级状态失败：' + String(e), 'warning')
        return null
      }
    },
    startUpdateStatusPolling() {
      if (this._updateStatusTimer) return
      this._updateStatusTimer = setInterval(async () => {
        const st = await this.refreshUpdateStatus(true)
        if (!st || st.running) return
        this.stopUpdateStatusPolling()
        if (st.status === 'failed') {
          this.notify(`升级失败：${st.last_error || st.message || '未知原因'}`, 'warning', 10000)
        } else if (st.status === 'succeeded') {
          this.notify('升级成功，服务已完成切换', 'info', 6000)
        }
      }, 5000)
    },
    stopUpdateStatusPolling() {
      if (this._updateStatusTimer) {
        clearInterval(this._updateStatusTimer)
        this._updateStatusTimer = null
      }
    },
    async applyUpdate() {
      if (!this.updateInfo || !this.updateInfo.update_available) { alert('当前已是最新版本'); return }
      if (this.updateStatus && this.updateStatus.running) { alert('已有升级任务正在执行，请稍后重试'); return }
      if (!confirm(`确认升级到 ${this.updateInfo.latest_version}？升级后将自动重启当前进程。`)) return
      this.updateApplying = true
      try {
        const payload = { restart_after_update: true }
        const res = await apiFetch('/api/meta/update/apply', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
        if (!res.ok) {
          await this.showApiError('升级', res)
          return
        }
        const data = await res.json()
        await this.refreshUpdateStatus(true)
        if (data && data.mode === 'external-script') {
          this.notify('升级任务已提交，正在后台执行，请关注升级状态', 'info', 8000)
          this.startUpdateStatusPolling()
          return
        }
        alert(`升级已执行：${data.message || 'ok'}。服务正在重启，请稍后刷新页面。`)
      } finally {
        this.updateApplying = false
      }
    },
    async loadDataSources() {
      try {
        const res = await apiFetch('/api/meta/data-sources', {}, this.apiKey)
        if (!res.ok) {
          this.notify(await this.extractApiError(res, '加载数据源列表失败'), 'warning')
          return
        }
        const data = await res.json()
        const nfa = Array.isArray(data.nfa) && data.nfa.length ? data.nfa : ['default']
        const edc = Array.isArray(data.edc) ? data.edc : []
        this.dataSourceCatalog = { nfa, edc }
        if (this.ui.showSourceAdmin) {
          await this.loadSourceAdminInstances()
        }
        const edcInst = this.getSourceInstances('edc')
        if (!edcInst.includes(this.bulkDefaults.data_source_instance)) {
          this.bulkDefaults.data_source_instance = edcInst[0]
        }
        this.onSourceTypeChange(this.newTask)
        if (this.editTask) this.onSourceTypeChange(this.editTask)
      } catch (e) {
        this.notify('加载数据源列表失败：' + String(e), 'warning')
      }
    },
    prettyJson(v) {
      try {
        return JSON.stringify(v || {}, null, 2)
      } catch (_) {
        return '{}'
      }
    },
    async loadSourceAdminInstances() {
      const st = this.sourceAdmin.source_type || 'all'
      const res = await apiFetch('/api/meta/data-sources/instances?source_type=' + encodeURIComponent(st), {}, this.apiKey)
      if (!res.ok) {
        this.notify(await this.extractApiError(res, '加载实例列表失败'), 'warning')
        return
      }
      const data = await res.json()
      const items = Array.isArray(data.items) ? data.items : []
      this.sourceAdmin.items = items
      if (items.length) {
        const cur = items.find((x) => x.instance === this.sourceAdmin.selected_instance) || items[0]
        this.sourceAdmin.selected_instance = cur.instance
        this.sourceAdmin.selected_source_type = cur.source_type || ''
        this.sourceAdmin.editor_text = this.prettyJson(cur.config || {})
      } else {
        this.sourceAdmin.selected_instance = ''
        this.sourceAdmin.selected_source_type = ''
        this.sourceAdmin.editor_text = '{}'
      }
      this.sourceAdmin.test_result = null
      await this.loadSourceAdminAudit()
      await this.loadRotatePolicy()
    },
    selectSourceAdminInstance(item) {
      if (!item) return
      this.sourceAdmin.selected_instance = item.instance
      this.sourceAdmin.selected_source_type = item.source_type || ''
      this.sourceAdmin.editor_text = this.prettyJson(item.config || {})
      this.sourceAdmin.test_result = null
    },
    async createSourceAdminInstance() {
      const st = this.sourceAdmin.source_type
      if (st === 'all') { alert('新建实例请先选择 NFA 或 EDC 类型'); return }
      const name = String(this.sourceAdmin.new_instance || '').trim()
      if (!name) { alert('请输入实例名'); return }
      let cfg = {}
      try {
        cfg = JSON.parse(this.sourceAdmin.editor_text || '{}')
      } catch (_) {
        alert('JSON 格式错误，请先修正')
        return
      }
      const payload = { source_type: st, instance: name, config: cfg }
      const res = await apiFetch('/api/meta/data-sources/instances', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('创建实例', res)
        return
      }
      this.sourceAdmin.new_instance = ''
      await this.loadSourceAdminInstances()
      this.sourceAdmin.selected_instance = name
      await this.loadDataSources()
    },
    async saveSourceAdminInstance() {
      const st = (this.sourceAdmin.source_type === 'all' ? this.sourceAdmin.selected_source_type : this.sourceAdmin.source_type) || 'edc'
      const name = String(this.sourceAdmin.selected_instance || '').trim()
      if (!name) { alert('请先选择实例'); return }
      let cfg = {}
      try {
        cfg = JSON.parse(this.sourceAdmin.editor_text || '{}')
      } catch (_) {
        alert('JSON 格式错误，请先修正')
        return
      }
      const payload = { source_type: st, instance: name, config: cfg }
      const res = await apiFetch('/api/meta/data-sources/instances', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('保存实例', res)
        return
      }
      await this.loadSourceAdminInstances()
      await this.loadDataSources()
      alert('保存成功')
    },
    async deleteSourceAdminInstance() {
      const st = (this.sourceAdmin.source_type === 'all' ? this.sourceAdmin.selected_source_type : this.sourceAdmin.source_type) || 'edc'
      const name = String(this.sourceAdmin.selected_instance || '').trim()
      if (!name) { alert('请先选择实例'); return }
      if (!confirm(`确认删除实例 ${name}？`)) return
      const q = new URLSearchParams({ source_type: st, instance: name }).toString()
      const res = await apiFetch('/api/meta/data-sources/instances?' + q, { method: 'DELETE' }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('删除实例', res)
        return
      }
      await this.loadSourceAdminInstances()
      await this.loadDataSources()
    },
    async testSourceAdminConnection() {
      const st = (this.sourceAdmin.source_type === 'all' ? this.sourceAdmin.selected_source_type : this.sourceAdmin.source_type) || 'edc'
      const name = String(this.sourceAdmin.selected_instance || '').trim()
      let cfg = null
      try {
        cfg = JSON.parse(this.sourceAdmin.editor_text || '{}')
      } catch (_) {
        alert('JSON 格式错误，请先修正')
        return
      }
      const payload = { source_type: st, instance: name || null, config: cfg }
      const res = await apiFetch('/api/meta/data-sources/test', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        const msg = await this.extractApiError(res, '连接测试失败')
        this.sourceAdmin.test_result = { ok: false, message: msg, elapsed_ms: 0 }
        return
      }
      const data = await res.json()
      this.sourceAdmin.test_result = data
    },
    async loadSourceAdminAudit() {
      const n = Number(this.sourceAdmin.audit_limit || 50)
      const res = await apiFetch('/api/meta/data-sources/audit?limit=' + encodeURIComponent(String(n)), {}, this.apiKey)
      if (!res.ok) {
        this.notify(await this.extractApiError(res, '加载配置审计失败'), 'warning')
        return
      }
      const data = await res.json()
      this.sourceAdmin.audit_items = Array.isArray(data.items) ? data.items : []
    },
    async rotateSourceAdminKey() {
      const oldSeed = String(this.sourceAdmin.rotate_old_seed || '').trim()
      const newSeed = String(this.sourceAdmin.rotate_new_seed || '').trim()
      if (!oldSeed || !newSeed) { alert('请填写旧密钥与新密钥'); return }
      if (!confirm('确认执行配置加密密钥轮换？执行后请同步更新 CONFIG_ENCRYPTION_KEY 并重启服务。')) return
      const payload = { old_seed: oldSeed, new_seed: newSeed }
      const res = await apiFetch('/api/meta/data-sources/rotate-key', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('密钥轮换', res)
        return
      }
      const data = await res.json()
      this.sourceAdmin.rotate_old_seed = ''
      this.sourceAdmin.rotate_new_seed = ''
      await this.loadSourceAdminAudit()
      alert(`密钥轮换成功，已重加密 ${Number(data.rotated || 0)} 条配置。请更新服务端 CONFIG_ENCRYPTION_KEY 并重启。`)
    },
    async loadRotatePolicy() {
      const res = await apiFetch('/api/meta/data-sources/rotate-policy', {}, this.apiKey)
      if (!res.ok) {
        this.notify(await this.extractApiError(res, '加载轮换策略失败'), 'warning')
        return
      }
      const data = await res.json()
      this.sourceAdmin.rotate_policy = {
        enabled: !!data.enabled,
        interval_days: Number(data.interval_days || 30),
        last_rotated_at: data.last_rotated_at || ''
      }
    },
    async saveRotatePolicy() {
      const p = this.sourceAdmin.rotate_policy || {}
      const payload = {
        enabled: !!p.enabled,
        interval_days: Math.max(1, Number(p.interval_days || 30))
      }
      const res = await apiFetch('/api/meta/data-sources/rotate-policy', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('保存轮换策略', res)
        return
      }
      const data = await res.json()
      this.sourceAdmin.rotate_policy = {
        enabled: !!data.enabled,
        interval_days: Number(data.interval_days || 30),
        last_rotated_at: data.last_rotated_at || ''
      }
      alert('轮换策略已保存')
    },
    async rotateSourceAdminKeyAutoNow() {
      if (!confirm('确认立即执行一次自动密钥轮换？')) return
      const res = await apiFetch('/api/meta/data-sources/rotate-key-auto?force=true', { method: 'POST' }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('自动轮换', res)
        return
      }
      const data = await res.json()
      await this.loadRotatePolicy()
      await this.loadSourceAdminAudit()
      if (data.rotated) {
        alert(`自动轮换成功，已重加密 ${Number(data.count || 0)} 条配置`)
      } else {
        alert(`未执行轮换：${data.reason || 'not_due'}`)
      }
    },
    getSourceInstances(sourceType) {
      const st = (sourceType || 'nfa').toLowerCase()
      const arr = this.dataSourceCatalog[st]
      if (Array.isArray(arr) && arr.length) return arr
      return ['default']
    },
    onSourceTypeChange(obj) {
      if (!obj) return
      const options = this.getSourceInstances(obj.data_source_type)
      if (!options.includes(obj.data_source_instance)) {
        obj.data_source_instance = options[0]
      }
      this.normalizeSourceParams(obj)
    },
    onCombineV4V6Toggle(obj) {
      if (!obj || !obj.params) return
      const p = obj.params
      p.combine_v4_v6 = !!p.combine_v4_v6
      if (!p.combine_v4_v6) {
        p.merge_key = ''
      }
    },
    defaultTaskParams() {
      return {
        direction: 'both',
        export_daily: false,
        sortby: '',
        sort_order: 'desc',
        aggregate_all: false,
        combine_v4_v6: false,
        merge_key: '',
        batch_size: 200,
        school: '',
        exclude_school: '',
        unit_base: 1024,
        settlement_mode: 'range_95',
        monthly_aggregate: false,
        data_budget_enabled: false,
        data_budget_mul: 8,
        data_budget_div: 300,
        edc_name: '',
        edc_match_mode: 'prefix'
      }
    },
    normalizeSourceParams(obj) {
      if (!obj) return
      const st = (obj.data_source_type || 'nfa').toLowerCase()
      const p = obj.params = (obj.params || {})
      const d = this.defaultTaskParams()
      for (const k of Object.keys(d)) {
        if (p[k] == null) p[k] = d[k]
      }
      if (!p.sort_order || (p.sort_order !== 'asc' && p.sort_order !== 'desc')) p.sort_order = 'desc'
      if (p.unit_base !== 1000 && p.unit_base !== 1024) p.unit_base = 1024
      if (!p.settlement_mode) p.settlement_mode = 'range_95'
      if (p.batch_size == null || p.batch_size === '') p.batch_size = 200
      p.export_daily = !!p.export_daily
      p.monthly_aggregate = !!p.monthly_aggregate
      if (st === 'edc') {
        p.direction = 'both'
        p.aggregate_all = false
        p.combine_v4_v6 = false
        p.merge_key = ''
        p.school = ''
        p.exclude_school = ''
        p.batch_size = 200
        if (typeof p.data_budget_enabled !== 'boolean') p.data_budget_enabled = false
        delete p.province
        delete p.cp
        if (p.data_budget_mul == null || p.data_budget_mul === '') p.data_budget_mul = 8
        if (p.data_budget_div == null || p.data_budget_div === '') p.data_budget_div = 300
        if (p.edc_match_mode !== 'exact' && p.edc_match_mode !== 'prefix') p.edc_match_mode = 'prefix'
      } else {
        if (!p.direction) p.direction = 'both'
        if (typeof p.aggregate_all !== 'boolean') p.aggregate_all = false
        if (typeof p.combine_v4_v6 !== 'boolean') p.combine_v4_v6 = false
        if (typeof p.merge_key !== 'string') p.merge_key = p.merge_key == null ? '' : String(p.merge_key)
        p.merge_key = p.merge_key.trim()
        if (!p.combine_v4_v6) p.merge_key = ''
        if (!p.batch_size || Number(p.batch_size) < 10) p.batch_size = 200
        delete p.edc_name
        delete p.edc_match_mode
        delete p.data_budget_enabled
        delete p.data_budget_mul
        delete p.data_budget_div
      }
    },
    tokenizeShell(line) {
      const out = []
      const re = /"([^"]*)"|'([^']*)'|(\S+)/g
      let m
      while ((m = re.exec(line)) !== null) {
        out.push(m[1] ?? m[2] ?? m[3] ?? '')
      }
      return out
    },
    extractEdcFromCommandLine(line) {
      const tokens = this.tokenizeShell(line)
      if (!tokens.length) return ''
      const idx = tokens.findIndex((t) => /(?:^|[\\/])day-95(?:-ali)?\.sh$/i.test(t))
      if (idx >= 0 && idx + 1 < tokens.length) {
        const v = String(tokens[idx + 1] || '').trim()
        if (v && !v.startsWith('-')) return v
      }
      for (const t of tokens) {
        const v = String(t || '').trim()
        if (!v || v.startsWith('-')) continue
        if (v.includes('*') || v.includes('?')) return v
      }
      return ''
    },
    async parseBulkImportCommands() {
      const lines = String(this.bulkImportText || '').split(/\r?\n/)
      const items = []
      let n = 1
      for (const rawLine of lines) {
        const line = rawLine.trim()
        if (!line || line.startsWith('#')) continue
        const edc = this.extractEdcFromCommandLine(line)
        if (!edc) continue
        items.push({
          id: `imp-${Date.now()}-${n}`,
          selected: true,
          edc_name: edc,
          name: edc,
          schedule_expr: '',
          conflicts: []
        })
        n++
      }
      this.bulkImportItems = items
      await this.precheckBulkImport()
      if (!items.length) alert('未识别到可导入的 EDC 命令，请检查命令格式')
    },
    clearBulkImport() {
      this.bulkImportText = ''
      this.bulkImportItems = []
      this.bulkPrecheckSummary = { total: 0, conflict_items: 0, name_conflict: 0, key_conflict: 0, batch_name_conflict: 0, batch_key_conflict: 0 }
    },
    normalizeEdcKey(edcName, instance) {
      return `${String(instance || '').trim().toLowerCase()}::${String(edcName || '').trim().toLowerCase()}`
    },
    async precheckBulkImport() {
      if (!this.bulkImportItems.length) {
        this.bulkPrecheckSummary = { total: 0, conflict_items: 0, name_conflict: 0, key_conflict: 0, batch_name_conflict: 0, batch_key_conflict: 0 }
        return
      }
      const res = await apiFetch('/api/tasks', {}, this.apiKey)
      if (!res.ok) {
        await this.showApiError('预检任务冲突', res, '无法加载现有任务')
        return
      }
      const existing = await res.json()
      const nameSet = new Set()
      const keySet = new Set()
      for (const t of (existing || [])) {
        const nm = String((t && t.name) || '').trim().toLowerCase()
        if (nm) nameSet.add(nm)
        if (String((t && t.data_source_type) || '').toLowerCase() === 'edc') {
          const inst = String((t && t.data_source_instance) || '').trim()
          const edc = String((((t && t.params) || {}).edc_name) || '').trim()
          if (inst && edc) keySet.add(this.normalizeEdcKey(edc, inst))
        }
      }
      const batchNameCount = new Map()
      const batchKeyCount = new Map()
      for (const it of this.bulkImportItems) {
        const nm = String(it.name || '').trim().toLowerCase()
        const key = this.normalizeEdcKey(it.edc_name, this.bulkDefaults.data_source_instance)
        if (nm) batchNameCount.set(nm, (batchNameCount.get(nm) || 0) + 1)
        if (String(it.edc_name || '').trim()) batchKeyCount.set(key, (batchKeyCount.get(key) || 0) + 1)
      }

      let conflictItems = 0
      let nameConflict = 0
      let keyConflict = 0
      let batchNameConflict = 0
      let batchKeyConflict = 0
      for (const it of this.bulkImportItems) {
        const conflicts = []
        const nm = String(it.name || '').trim().toLowerCase()
        const key = this.normalizeEdcKey(it.edc_name, this.bulkDefaults.data_source_instance)
        if (nm && nameSet.has(nm)) { conflicts.push('任务名已存在'); nameConflict++ }
        if (String(it.edc_name || '').trim() && keySet.has(key)) { conflicts.push('EDC名称+实例已存在'); keyConflict++ }
        if (nm && (batchNameCount.get(nm) || 0) > 1) { conflicts.push('本次导入中任务名重复'); batchNameConflict++ }
        if (String(it.edc_name || '').trim() && (batchKeyCount.get(key) || 0) > 1) { conflicts.push('本次导入中 EDC名称+实例 重复'); batchKeyConflict++ }
        it.conflicts = conflicts
        if (conflicts.length) conflictItems++
      }
      this.bulkPrecheckSummary = {
        total: this.bulkImportItems.length,
        conflict_items: conflictItems,
        name_conflict: nameConflict,
        key_conflict: keyConflict,
        batch_name_conflict: batchNameConflict,
        batch_key_conflict: batchKeyConflict
      }
    },
    bulkTaskPayload(item) {
      const d = this.bulkDefaults
      const params = {
        edc_name: String(item.edc_name || '').trim(),
        edc_match_mode: d.edc_match_mode === 'exact' ? 'exact' : 'prefix',
        direction: 'both',
        export_daily: false,
        sort_order: 'desc',
        unit_base: Number(d.unit_base || 1024),
        settlement_mode: d.settlement_mode || 'daily_95_avg',
        data_budget_enabled: !!d.data_budget_enabled,
        data_budget_mul: Number(d.data_budget_mul || 8),
        data_budget_div: Number(d.data_budget_div || 300)
      }
      const window_params = {}
      if (d.window_selector === 'last_n_days') {
        window_params.n = Number(d.window_n_days || 30)
      }
      const kind = d.kind || 'periodic'
      const payload = {
        name: String(item.name || params.edc_name),
        active: !!d.active,
        kind,
        data_source_type: 'edc',
        data_source_instance: d.data_source_instance || 'ali',
        schedule_type: kind === 'periodic' ? d.schedule_type : null,
        schedule_expr: kind === 'periodic' ? (item.schedule_expr || d.schedule_expr || null) : null,
        schedule_time_of_day: kind === 'periodic' ? (d.schedule_time_of_day || null) : null,
        timezone: d.timezone || 'Asia/Shanghai',
        window_selector: d.window_selector || 'last_month',
        window_params,
        params,
        export_formats: Array.isArray(d.export_formats) && d.export_formats.length ? d.export_formats : ['csv'],
        output_filename_template: d.output_filename_template || ''
      }
      return payload
    },
    async importSelectedBulkTasks() {
      await this.precheckBulkImport()
      const selected = this.bulkImportItems.filter((x) => x.selected && x.edc_name)
      if (!selected.length) { alert('请先勾选要导入的任务'); return }
      const selectedConflict = selected.filter((x) => Array.isArray(x.conflicts) && x.conflicts.length > 0)
      if (selectedConflict.length) {
        const okConflict = confirm(`选中项里有 ${selectedConflict.length} 条存在冲突，仍继续导入吗？`)
        if (!okConflict) return
      }
      const failures = []
      let ok = 0
      for (const it of selected) {
        const payload = this.bulkTaskPayload(it)
        try {
          const res = await apiFetch('/api/tasks', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
          if (res.ok) {
            ok++
          } else {
            const msg = await this.extractApiError(res, '导入失败')
            failures.push(`${payload.name}: ${msg}`)
          }
        } catch (e) {
          failures.push(`${payload.name}: ${String(e)}`)
        }
      }
      await this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
      await this.loadTaskGroups()
      let msg = `导入完成：成功 ${ok}，失败 ${failures.length}`
      if (failures.length) {
        msg += `\n失败示例：\n${failures.slice(0, 5).join('\n')}`
      }
      alert(msg)
      await this.precheckBulkImport()
    },
    async loadTasks() {
      await this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
      await this.loadTaskGroups()
    },
    async loadTaskGroups() {
      const res = await apiFetch('/api/tasks/groups', {}, this.apiKey)
      if (!res.ok) { return }
      const data = await res.json()
      this.taskGroups = Array.isArray(data.items) ? data.items : []
      if (this.tasksQuery.task_group && !this.taskGroups.includes(this.tasksQuery.task_group)) {
        this.tasksQuery.task_group = ''
      }
      if (this.groupAdmin.selected_name && !this.taskGroups.includes(this.groupAdmin.selected_name)) {
        this.groupAdmin.selected_name = ''
        this.groupAdmin.rename_to = ''
      }
    },
    async createTaskGroup() {
      const name = String(this.groupAdmin.new_name || '').trim()
      if (!name) { alert('请输入分组名'); return }
      const res = await apiFetch('/api/tasks/groups', { method: 'POST', body: JSON.stringify({ name }) }, this.apiKey)
      if (!res.ok) { await this.showApiError('创建分组', res); return }
      this.groupAdmin.new_name = ''
      await this.loadTaskGroups()
      this.groupAdmin.selected_name = name
      this.groupAdmin.rename_to = name
      this.notify('分组创建成功', 'info', 2000)
    },
    onSelectGroupAdmin() {
      this.groupAdmin.rename_to = this.groupAdmin.selected_name || ''
    },
    async renameTaskGroup() {
      const oldName = String(this.groupAdmin.selected_name || '').trim()
      const newName = String(this.groupAdmin.rename_to || '').trim()
      if (!oldName) { alert('请先选择分组'); return }
      if (!newName) { alert('请输入新分组名'); return }
      let merge = false
      if (oldName !== newName && this.taskGroups.includes(newName)) {
        if (!confirm(`目标分组「${newName}」已存在，确认合并到该分组吗？`)) return
        merge = true
      }
      const payload = { old_name: oldName, new_name: newName, merge }
      const res = await apiFetch('/api/tasks/groups/rename', { method: 'PATCH', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) { await this.showApiError('重命名分组', res); return }
      await this.loadTaskGroups()
      this.groupAdmin.selected_name = newName
      this.groupAdmin.rename_to = newName
      await this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
      this.notify('分组重命名成功', 'info', 2200)
    },
    async deleteTaskGroup() {
      const name = String(this.groupAdmin.selected_name || '').trim()
      if (!name) { alert('请先选择分组'); return }
      if (!confirm(`确认删除分组「${name}」？该分组下任务会变为未分组。`)) return
      const res = await apiFetch('/api/tasks/groups?name=' + encodeURIComponent(name), { method: 'DELETE' }, this.apiKey)
      if (!res.ok) { await this.showApiError('删除分组', res); return }
      this.groupAdmin.selected_name = ''
      this.groupAdmin.rename_to = ''
      await this.loadTaskGroups()
      await this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
      this.notify('分组删除成功', 'info', 2200)
    },
    async quickAssignTaskGroup(task, value) {
      if (!task || !task.id) return
      const next = String(value || '').trim()
      const target = next || null
      const current = task.group_name ? String(task.group_name) : null
      if (target === current) return
      const prev = current
      task.group_name = target
      this.groupAssignSavingTaskId = Number(task.id)
      try {
        const payload = { group_name: target }
        const res = await apiFetch('/api/tasks/' + task.id + '/group', { method: 'PATCH', body: JSON.stringify(payload) }, this.apiKey)
        if (!res.ok) {
          task.group_name = prev
          await this.showApiError('修改任务分组', res)
          return
        }
        const data = await res.json()
        task.group_name = data.group_name || null
        await this.loadTaskGroups()
        if (this.tasksQuery.task_group) {
          await this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
        }
      } finally {
        this.groupAssignSavingTaskId = null
      }
    },
    async loadTasksPage(page = 1, pageSize = 20) {
      const params = new URLSearchParams()
      params.set('page', String(page))
      params.set('page_size', String(pageSize))
      if (this.tasksQuery && this.tasksQuery.q) params.set('q', this.tasksQuery.q)
      if (this.tasksQuery && this.tasksQuery.task_kind && this.tasksQuery.task_kind !== 'all') params.set('task_kind', this.tasksQuery.task_kind)
      if (this.tasksQuery && this.tasksQuery.task_group) params.set('task_group', this.tasksQuery.task_group)
      if (this.tasksQuery && this.tasksQuery.sort_by) params.set('sort_by', this.tasksQuery.sort_by)
      if (this.tasksQuery && this.tasksQuery.sort_order) params.set('sort_order', this.tasksQuery.sort_order)
      const res = await apiFetch('/api/tasks/page?' + params.toString(), {}, this.apiKey)
      if (!res.ok) { await this.showApiError('任务列表加载', res); return }
      const data = await res.json()
      this.tasksPage = { items: data.items || [], total: data.total || 0, page: data.page || page, page_size: data.page_size || pageSize }
      this.tasks = this.tasksPage.items
      const exists = new Set(this.tasks.map((t) => Number(t.id)))
      this.selectedTaskIds = this.selectedTaskIds.filter((id) => exists.has(Number(id)))
      this.tasksPageJump = this.tasksPage.page
    },
    async createTask() {
      // 先规范化自定义时间窗口
      this.normalizeSourceParams(this.newTask)
      this.normalizeCustomWindow(this.newTask)
      if (!this.validateTask(this.newTask)) return
      const payload = JSON.parse(JSON.stringify(this.newTask))
      this.normalizeSourceParams(payload)
      this.normalizeCustomWindow(payload)
      this._deepTrimValue(payload)
      const res = await apiFetch('/api/tasks', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) { await this.showApiError('创建任务', res); return }
      await this.loadTasks()
    },
    startEdit(t) {
      // 深拷贝并填充默认值
      const clone = JSON.parse(JSON.stringify(t))
      clone.kind = clone.kind || 'one_off'
      clone.data_source_type = clone.data_source_type || 'nfa'
      clone.data_source_instance = clone.data_source_instance || 'default'
      clone.window_selector = clone.window_selector || 'custom'
      clone.window_params = clone.window_params || {}
      // 回填 datetime-local 显示值
      this.normalizeCustomWindow(clone)
      clone.params = clone.params || {}
      clone.export_formats = clone.export_formats && clone.export_formats.length ? clone.export_formats : ['csv']
      clone.timezone = clone.timezone || 'Asia/Shanghai'
      this.onSourceTypeChange(clone)
      this.editTask = clone
      this.$nextTick(() => { if (this.editTask && this.editTask.window_selector === 'custom') this.initRangePickerEdit() })
    },
    cancelEdit() {
      this.editTask = null
    },
    async saveEdit() {
      if (!this.editTask || !this.editTask.id) return
      // 先规范化自定义时间窗口
      this.normalizeSourceParams(this.editTask)
      this.normalizeCustomWindow(this.editTask)
      if (!this.validateTask(this.editTask)) return
      const allowKeys = ['name','group_name','active','kind','data_source_type','data_source_instance','schedule_type','schedule_expr','schedule_time_of_day','timezone','window_selector','window_params','params','export_formats','output_filename_template']
      const body = {}
      for (const k of allowKeys) { if (k in this.editTask) body[k] = this.editTask[k] }
      this.normalizeSourceParams(body)
      this.normalizeCustomWindow(body)
      this._deepTrimValue(body)
      const res = await apiFetch('/api/tasks/' + this.editTask.id, { method: 'PUT', body: JSON.stringify(body) }, this.apiKey)
      if (!res.ok) { await this.showApiError('保存任务', res); return }
      this.editTask = null
      await this.loadTasks()
    },
    _deepTrimValue(v) {
      if (v == null) return v
      if (typeof v === 'string') return v.trim()
      if (Array.isArray(v)) {
        for (let i = 0; i < v.length; i++) {
          v[i] = this._deepTrimValue(v[i])
        }
        return v
      }
      if (typeof v === 'object') {
        for (const k of Object.keys(v)) {
          v[k] = this._deepTrimValue(v[k])
        }
        return v
      }
      return v
    },
    async removeTask(id) {
      if (!confirm('确认删除?')) return
      const res = await apiFetch('/api/tasks/' + id, { method: 'DELETE' }, this.apiKey)
      if (!res.ok) { await this.showApiError('删除任务', res); return }
      const curPage = this.tasksPage.page
      await this.loadTasksPage(curPage, this.tasksPage.page_size)
      if (this.tasks.length === 0 && curPage > 1) {
        await this.loadTasksPage(curPage - 1, this.tasksPage.page_size)
      }
      await this.loadTaskGroups()
    },
    async runTask(id) {
      const res = await apiFetch('/api/tasks/' + id + '/run', { method: 'POST' }, this.apiKey)
      if (!res.ok) { await this.showApiError('触发任务', res); return }
      const data = await res.json(); alert('已触发: ' + data.job_id)
      await this.loadRuns()
    },
    async viewRuns(taskId) {
      this.runsFilterTaskId = taskId
      await this.loadRunsPage(1, this.runsPage.page_size)
    },
    async loadRuns() {
      await this.loadRunsPage(this.runsPage.page, this.runsPage.page_size)
    },
    async loadRunsPage(page = 1, pageSize = 20) {
      const params = new URLSearchParams()
      params.set('page', String(page))
      params.set('page_size', String(pageSize))
      if (this.runsFilterTaskId != null) params.set('task_id', String(this.runsFilterTaskId))
      if (this.runsQuery && this.runsQuery.status) params.set('status', this.runsQuery.status)
      if (this.runsQuery && this.runsQuery.month) params.set('month', this.runsQuery.month)
      if (this.runsQuery && this.runsQuery.data_month) params.set('data_month', this.runsQuery.data_month)
      if (this.runsQuery && this.runsQuery.task_kind && this.runsQuery.task_kind !== 'all') params.set('task_kind', this.runsQuery.task_kind)
      if (this.runsQuery && this.runsQuery.sort_by) params.set('sort_by', this.runsQuery.sort_by)
      if (this.runsQuery && this.runsQuery.sort_order) params.set('sort_order', this.runsQuery.sort_order)
      const res = await apiFetch('/api/jobs/page?' + params.toString(), {}, this.apiKey)
      if (!res.ok) { await this.showApiError('加载运行记录', res); return }
      const data = await res.json()
      this.runsPage = { items: data.items || [], total: data.total || 0, page: data.page || page, page_size: data.page_size || pageSize }
      this.runs = this.runsPage.items
      const exists = new Set(this.runs.map((r) => String(r.id)))
      this.selectedRunIds = this.selectedRunIds.filter((id) => exists.has(String(id)))
      this.runsPageJump = this.runsPage.page
      this.previewBatchDownload()
    },
    applyTasksQuery() {
      this.loadTasksPage(1, this.tasksPage.page_size)
    },
    clearTasksQuery() {
      this.tasksQuery.q = ''
      this.tasksQuery.task_kind = 'all'
      this.tasksQuery.task_group = ''
      this.loadTasksPage(1, this.tasksPage.page_size)
    },
    applyRunsQuery() {
      this.loadRunsPage(1, this.runsPage.page_size)
      this.previewBatchDownload()
    },
    clearRunsFilter() {
      this.runsFilterTaskId = null
      this.runsQuery.status = ''
      this.runsQuery.month = ''
      this.runsQuery.data_month = ''
      this.runsQuery.task_kind = 'all'
      this.downloadPreview = { matched_runs: 0, matched_files: 0 }
      this.loadRunsPage(1, this.runsPage.page_size)
    },
    nextTasksPage() {
      const totalPages = this.totalPages(this.tasksPage.total, this.tasksPage.page_size)
      if (this.tasksPage.page < totalPages) this.loadTasksPage(this.tasksPage.page + 1, this.tasksPage.page_size)
    },
    prevTasksPage() {
      if (this.tasksPage.page > 1) this.loadTasksPage(this.tasksPage.page - 1, this.tasksPage.page_size)
    },
    nextRunsPage() {
      const totalPages = this.totalPages(this.runsPage.total, this.runsPage.page_size)
      if (this.runsPage.page < totalPages) this.loadRunsPage(this.runsPage.page + 1, this.runsPage.page_size)
    },
    prevRunsPage() {
      if (this.runsPage.page > 1) this.loadRunsPage(this.runsPage.page - 1, this.runsPage.page_size)
    },
    changeTasksPageSize() {
      this.loadTasksPage(1, this.tasksPage.page_size)
    },
    changeRunsPageSize() {
      this.loadRunsPage(1, this.runsPage.page_size)
    },
    jumpTasksPage() {
      const totalPages = this.totalPages(this.tasksPage.total, this.tasksPage.page_size)
      let p = Number(this.tasksPageJump) || 1
      if (p < 1) p = 1
      if (p > totalPages) p = totalPages
      this.loadTasksPage(p, this.tasksPage.page_size)
    },
    jumpRunsPage() {
      const totalPages = this.totalPages(this.runsPage.total, this.runsPage.page_size)
      let p = Number(this.runsPageJump) || 1
      if (p < 1) p = 1
      if (p > totalPages) p = totalPages
      this.loadRunsPage(p, this.runsPage.page_size)
    },
    formatDateTime(dt) {
      if (!dt) return '-'
      try {
        const d = this._parseServerTime(dt)
        return d ? d.toLocaleString() : String(dt)
      } catch { return String(dt) }
    },
    shortId(v, n = 8) {
      if (!v) return '-'
      const s = String(v)
      if (s.length <= n) return s
      return s.slice(0, n) + '...'
    },
    formatDateTimeShort(dt) {
      if (!dt) return '-'
      const d = this._parseServerTime(dt)
      if (!d) {
        const s = String(dt).replace('T', ' ')
        return s.length > 16 ? s.slice(0, 16) : s
      }
      const p = (x) => String(x).padStart(2, '0')
      return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
    },
    _parseServerTime(v) {
      if (!v) return null
      let s = String(v).trim()
      if (!s) return null
      // with timezone info, parse directly
      if (/[zZ]$/.test(s) || /[+\-]\d{2}:\d{2}$/.test(s)) {
        const d1 = new Date(s)
        return Number.isNaN(d1.getTime()) ? null : d1
      }
      // naive timestamp from backend is treated as UTC
      s = s.replace(' ', 'T') + 'Z'
      const d2 = new Date(s)
      return Number.isNaN(d2.getTime()) ? null : d2
    },
    formatBudgetValue(v, base = 1000) {
      let n = Number(v || 0)
      if (!Number.isFinite(n)) n = 0
      const b = Number(base) === 1024 ? 1024 : 1000
      // budget values are in Mbps; UI统一展示为 Gbps
      return `${(n / b).toFixed(4)} Gbps`
    },
    runsByStatus(status) {
      const s = String(status || '')
      return (this.runs || []).filter((r) => String(r.status || '') === s)
    },
    timelineEvents(r) {
      const arr = Array.isArray(r && r.progress_events) ? r.progress_events : []
      return arr.slice(-6)
    },
    runDiagnosticsArtifact(r) {
      const arr = Array.isArray(r && r.artifacts) ? r.artifacts : []
      return arr.find((a) => String(a && a.artifact_kind || '') === 'diagnostics') || null
    },
    async viewRunLog(run) {
      if (!run || !run.id) return
      const jobId = String(run.id)
      this.logViewer = { visible: true, jobId, content: '', loading: true, error: '' }
      try {
        const res = await apiFetch('/api/jobs/' + encodeURIComponent(jobId) + '/log-tail?lines=200', {}, this.apiKey)
        if (!res.ok) {
          const msg = await this.extractApiError(res, '加载日志失败')
          this.logViewer.loading = false
          this.logViewer.error = msg
          return
        }
        const data = await res.json()
        this.logViewer.content = String((data && data.content) || '')
        this.logViewer.loading = false
      } catch (e) {
        this.logViewer.loading = false
        this.logViewer.error = String(e)
      }
    },
    closeLogViewer() {
      this.logViewer = { visible: false, jobId: '', content: '', loading: false, error: '' }
    },
    async viewEdcMatch(run) {
      if (!run || !run.id) return
      this.matchViewer = { visible: true, loading: true, error: '', data: null }
      try {
        const res = await apiFetch('/api/jobs/' + encodeURIComponent(run.id) + '/edc-match', {}, this.apiKey)
        if (!res.ok) {
          this.matchViewer.loading = false
          this.matchViewer.error = await this.extractApiError(res, '加载匹配快照失败')
          return
        }
        this.matchViewer.data = await res.json()
        this.matchViewer.loading = false
      } catch (e) {
        this.matchViewer.loading = false
        this.matchViewer.error = String(e)
      }
    },
    closeMatchViewer() {
      this.matchViewer = { visible: false, loading: false, error: '', data: null }
    },
    taskChecked(id) {
      return this.selectedTaskIds.includes(Number(id))
    },
    runChecked(id) {
      return this.selectedRunIds.includes(String(id))
    },
    toggleTaskSelect(id) {
      const n = Number(id)
      if (this.selectedTaskIds.includes(n)) {
        this.selectedTaskIds = this.selectedTaskIds.filter((x) => x !== n)
      } else {
        this.selectedTaskIds.push(n)
      }
    },
    toggleRunSelect(id) {
      const s = String(id)
      if (this.selectedRunIds.includes(s)) {
        this.selectedRunIds = this.selectedRunIds.filter((x) => x !== s)
      } else {
        this.selectedRunIds.push(s)
      }
      this.previewBatchDownload()
    },
    selectAllTasksOnPage() {
      this.selectedTaskIds = this.tasks.map((t) => Number(t.id))
    },
    clearTaskSelection() {
      this.selectedTaskIds = []
    },
    selectAllRunsOnPage() {
      this.selectedRunIds = this.runs.map((r) => String(r.id))
      this.previewBatchDownload()
    },
    clearRunSelection() {
      this.selectedRunIds = []
      this.previewBatchDownload()
    },
    async batchDeleteTasks() {
      if (!this.selectedTaskIds.length) { alert('请先选择任务'); return }
      if (!confirm(`确认删除选中的 ${this.selectedTaskIds.length} 个任务？`)) return
      const res = await apiFetch('/api/tasks/batch-delete', { method: 'POST', body: JSON.stringify({ ids: this.selectedTaskIds }) }, this.apiKey)
      if (!res.ok) { await this.showApiError('批量删除任务', res); return }
      this.selectedTaskIds = []
      await this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
      await this.loadTaskGroups()
    },
    async batchRunTasks() {
      if (!this.selectedTaskIds.length) { alert('请先选择任务'); return }
      const ids = [...this.selectedTaskIds]
      let ok = 0
      const failures = []
      for (const id of ids) {
        try {
          const res = await apiFetch('/api/tasks/' + id + '/run', { method: 'POST' }, this.apiKey)
          if (res.ok) {
            ok++
          } else {
            const msg = await this.extractApiError(res, '触发失败')
            failures.push(`#${id}: ${msg}`)
          }
        } catch (e) {
          failures.push(`#${id}: ${String(e)}`)
        }
      }
      await this.loadRunsPage(1, this.runsPage.page_size)
      let msg = `批量运行完成：成功 ${ok}，失败 ${failures.length}`
      if (failures.length) msg += `\n失败示例：\n${failures.slice(0, 5).join('\n')}`
      alert(msg)
    },
    async batchDeleteRuns() {
      if (!this.selectedRunIds.length) { alert('请先选择运行记录'); return }
      if (!confirm(`确认删除选中的 ${this.selectedRunIds.length} 条运行记录？`)) return
      const res = await apiFetch('/api/jobs/batch-delete', { method: 'POST', body: JSON.stringify({ ids: this.selectedRunIds }) }, this.apiKey)
      if (!res.ok) { await this.showApiError('批量删除运行记录', res); return }
      this.selectedRunIds = []
      await this.loadRunsPage(this.runsPage.page, this.runsPage.page_size)
    },
    async batchDownloadRuns() {
      const payload = {
        run_ids: this.selectedRunIds.length ? this.selectedRunIds : null,
        month: this.runsQuery.month || null,
        task_kind: this.runsQuery.task_kind || 'all',
        status: this.runsQuery.status || 'succeeded',
        file_format: this.runsQuery.file_format || 'csv'
      }
      const res = await apiFetch('/api/jobs/batch-download', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        await this.showApiError('批量下载', res)
        return
      }
      const blob = await res.blob()
      const cd = res.headers.get('Content-Disposition') || ''
      let filename = 'artifacts.zip'
      const m = cd.match(/filename="?([^\";]+)"?/)
      if (m && m[1]) filename = m[1]
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = filename
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    },
    async previewBatchDownload() {
      const payload = {
        run_ids: this.selectedRunIds.length ? this.selectedRunIds : null,
        month: this.runsQuery.month || null,
        task_kind: this.runsQuery.task_kind || 'all',
        status: this.runsQuery.status || 'succeeded',
        file_format: this.runsQuery.file_format || 'csv'
      }
      const res = await apiFetch('/api/jobs/batch-download/preview', { method: 'POST', body: JSON.stringify(payload) }, this.apiKey)
      if (!res.ok) {
        this.notify(await this.extractApiError(res, '下载预览计算失败'), 'warning', 2600)
        return
      }
      const data = await res.json()
      this.downloadPreview = {
        matched_runs: Number(data.matched_runs || 0),
        matched_files: Number(data.matched_files || 0)
      }
    },
    scheduleSummary(t) {
      if (!t) return ''
      if (t.kind !== 'periodic' || !t.active) return t.kind === 'periodic' ? '未启用' : '一次性'
      const st = t.schedule_type
      if (st === 'cron' && t.schedule_expr) return `cron: ${t.schedule_expr}`
      if (st === 'interval' && t.schedule_expr) return `每 ${t.schedule_expr}s`
      if (st === 'weekly_preset' && t.schedule_time_of_day) return `每周一 ${t.schedule_time_of_day}`
      return '未配置'
    },
    windowLabelForPreview(obj) {
      if (!obj) return ''
      const sel = obj.window_selector
      const pad2 = (x) => String(x).padStart(2, '0')
      const fmtYMD = (d) => `${d.getFullYear()}${pad2(d.getMonth()+1)}${pad2(d.getDate())}`
      const fmtY_M_D = (d) => `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`

      if (sel === 'custom') {
        // 同步一次，确保有标准字段
        this.normalizeCustomWindow(obj)
        const s = (obj.window_params && obj.window_params.start_time) || ''
        const e = (obj.window_params && obj.window_params.end_time) || ''
        const sd = s ? (s.split(' ')[0]) : ''
        const ed = e ? (e.split(' ')[0]) : ''
        return sd && ed ? `${sd}-${ed}` : 'custom'
      }
      if (sel === 'last_n_days') {
        const n = (obj.window_params && obj.window_params.n) || 7
        const now = new Date()
        // end = today 23:59:59 local; label uses end's date YYYYMMDD
        const end = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59)
        return `last${n}d-${fmtYMD(end)}`
      }
      if (sel === 'last_week') {
        const now = new Date()
        // JS: getDay() 0=Sun..6=Sat; we want Monday as 0 offset
        const day = now.getDay() // 0..6
        const daysSinceMonday = (day + 6) % 7
        // current week's Monday 00:00
        const thisMonday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - daysSinceMonday, 0, 0, 0)
        const lastMonday = new Date(thisMonday.getFullYear(), thisMonday.getMonth(), thisMonday.getDate() - 7, 0, 0, 0)
        const lastSunday = new Date(thisMonday.getFullYear(), thisMonday.getMonth(), thisMonday.getDate() - 1, 23, 59, 59)
        return `${fmtYMD(lastMonday)}-${fmtYMD(lastSunday)}`
      }
      if (sel === 'last_month') {
        const now = new Date()
        const firstOfThisMonth = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0)
        const end = new Date(firstOfThisMonth.getTime() - 1000)
        const start = new Date(end.getFullYear(), end.getMonth(), 1, 0, 0, 0)
        return `${fmtYMD(start)}-${fmtYMD(end)}`
      }
      return sel || ''
    },
    renderTemplate(template, obj) {
      const params = obj.params || {}
      const province = params.province || 'province'
      const cp = params.cp || 'cp'
      const direction = params.direction || 'both'
      const edcName = params.edc_name || 'edc'
      const source = obj.data_source_type || 'nfa'
      const instance = obj.data_source_instance || 'default'
      const windowLabel = this.windowLabelForPreview(obj)
      const today = new Date()
      const yyyy = today.getFullYear()
      const mm = String(today.getMonth() + 1).padStart(2, '0')
      const dd = String(today.getDate()).padStart(2, '0')
      const date = `${yyyy}-${mm}-${dd}`
      let out = template || ''
      out = out.replaceAll('{province}', province)
               .replaceAll('{cp}', cp)
               .replaceAll('{direction}', direction)
               .replaceAll('{edc}', edcName)
               .replaceAll('{source}', source)
               .replaceAll('{instance}', instance)
               .replaceAll('{window}', windowLabel)
               .replaceAll('{date}', date)
      return out
    },
    totalPages(total, pageSize) {
      const ps = Number(pageSize) || 1
      return Math.max(1, Math.ceil((Number(total) || 0) / ps))
    },
    sanitizeFilenamePart(name) {
      let s = String(name || '')
      s = s.replace(/[<>:"/\\|?*\x00-\x1f]/g, '_')
      s = s.replace(/[ .]+$/g, '')
      if (!s) s = 'artifact'
      const base = s.split('.')[0].toUpperCase()
      const reserved = new Set(['CON','PRN','AUX','NUL','COM1','COM2','COM3','COM4','COM5','COM6','COM7','COM8','COM9','LPT1','LPT2','LPT3','LPT4','LPT5','LPT6','LPT7','LPT8','LPT9'])
      if (reserved.has(base)) s = '_' + s
      return s
    },
    filenamePreview(obj) {
      if (!obj) return ''
      const tpl = obj.output_filename_template || ''
      if (tpl) return this.sanitizeFilenamePart(this.renderTemplate(tpl, obj))
      // default naming
      const params = obj.params || {}
      const province = params.province || 'province'
      const cp = params.cp || 'cp'
      const direction = params.direction || 'both'
      const source = (obj.data_source_type || 'nfa').toLowerCase()
      const windowLabel = this.windowLabelForPreview(obj)
      if (source === 'edc') {
        const edcName = params.edc_name || 'edc'
        const instance = obj.data_source_instance || 'default'
        return this.sanitizeFilenamePart(`${edcName}-${instance}-${windowLabel}`)
      }
      return this.sanitizeFilenamePart(`${province}-${cp}-${direction}-${windowLabel}`)
    },
    validateTask(obj) {
      // 基础校验：周期性任务需要完整调度字段
      if (obj.kind === 'periodic' && obj.active) {
        if (!obj.schedule_type) { alert('请选择调度类型'); return false }
        if (obj.schedule_type === 'cron' && !obj.schedule_expr) { alert('请填写 cron 表达式'); return false }
        if (obj.schedule_type === 'interval' && !obj.schedule_expr) { alert('请填写间隔秒数'); return false }
        if (obj.schedule_type === 'weekly_preset' && !obj.schedule_time_of_day) { alert('请填写每日执行时刻'); return false }
      }
      // 自定义时间范围时需要开始/结束
      if (obj.window_selector === 'custom') {
        this.normalizeCustomWindow(obj)
        const wp = obj.window_params || {}
        if (!wp.start_time || !wp.end_time) { alert('自定义时间范围需要填写开始与结束时间'); return false }
      }
      // batch_size 简单校验
      const ps = obj.params || {}
      const st = (obj.data_source_type || 'nfa').toLowerCase()
      if (st === 'nfa') {
        if (!ps.province || !ps.cp) { alert('NFA 任务必须填写省份和 CP'); return false }
      } else if (st === 'edc') {
        if (!ps.edc_name) { alert('EDC 任务必须填写 edc_name'); return false }
        if (ps.data_budget_enabled) {
          const mul = Number(ps.data_budget_mul)
          const div = Number(ps.data_budget_div)
          if (!Number.isFinite(mul)) { alert('数据预算公式中的乘数必须是数字'); return false }
          if (!Number.isFinite(div) || div === 0) { alert('数据预算公式中的除数必须是非零数字'); return false }
        }
      }
      if (st === 'nfa' && ps.batch_size != null) {
        const n = Number(ps.batch_size)
        if (!Number.isFinite(n) || n < 10) { alert('batch_size 需为 >=10 的数字'); return false }
      }
      // unit_base 校验
      if (ps.unit_base != null && ps.unit_base !== 1000 && ps.unit_base !== 1024) {
        alert('单位换算基数仅支持 1000 或 1024')
        return false
      }
      return true
    },
    async toggleActive(t) {
      const body = { active: !t.active }
      const res = await apiFetch('/api/tasks/' + t.id, { method: 'PUT', body: JSON.stringify(body) }, this.apiKey)
      if (!res.ok) { await this.showApiError('切换任务状态', res); return }
      await this.loadTasks()
    },
    async deleteRun(id) {
      if (!confirm('确认删除该运行记录及产物？')) return
      try {
        const res = await apiFetch('/api/jobs/' + id, { method: 'DELETE' }, this.apiKey)
        if (!res.ok) {
          await this.showApiError('删除运行记录', res)
          return
        }
        const curPage = this.runsPage.page
        await this.loadRunsPage(curPage, this.runsPage.page_size)
        if (this.runs.length === 0 && curPage > 1) {
          await this.loadRunsPage(curPage - 1, this.runsPage.page_size)
        }
      } catch (e) {
        console.error('删除请求异常', e)
        alert('删除请求异常：' + e)
      }
    }
  },
  watch: {
    apiKey() {
      this.loadDataSources()
    },
    'newTask.data_source_type'() {
      this.onSourceTypeChange(this.newTask)
    },
    'newTask.window_selector'(v) {
      if (v === 'custom') this.$nextTick(() => this.initRangePickerNew())
    },
    editTask(v) {
      if (v && v.window_selector === 'custom') this.$nextTick(() => this.initRangePickerEdit())
    },
    'editTask.window_selector'(v) {
      if (v === 'custom') this.$nextTick(() => this.initRangePickerEdit())
    },
    'editTask.data_source_type'() {
      if (this.editTask) this.onSourceTypeChange(this.editTask)
    },
    'sourceAdmin.source_type'(v) {
      if (v !== 'all') this.sourceAdmin.selected_source_type = v
      if (v === 'all') {
        this.loadSourceAdminInstances()
        return
      }
      this.loadSourceAdminInstances()
    },
    'ui.showSourceAdmin'(v) {
      if (v) this.loadSourceAdminInstances()
    }
  },
  mounted() {
    this.checkHealth()
    this.checkUpdate()
    this.refreshUpdateStatus(true)
    this.loadDataSources()
    this.loadTaskGroups()
    this.loadTasksPage(this.tasksPage.page, this.tasksPage.page_size)
    this.loadRunsPage(this.runsPage.page, this.runsPage.page_size)
    this.previewBatchDownload()
    this.$nextTick(() => { if (this.newTask.window_selector === 'custom') this.initRangePickerNew() })
    this._runTimer = setInterval(() => {
      if ((this.runs || []).some((r) => ['running', 'pending'].includes(String(r.status || '')))) {
        this.loadRunsPage(this.runsPage.page, this.runsPage.page_size)
      }
    }, 8000)
  },
  unmounted() {
    if (this._runTimer) clearInterval(this._runTimer)
    this.stopUpdateStatusPolling()
    if (this._noticeTimer) clearTimeout(this._noticeTimer)
  }
}).mount('#app')
}





