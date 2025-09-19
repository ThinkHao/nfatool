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
      tasks: [],
      runs: [],
      newTask: {
        name: '',
        active: true,
        kind: 'one_off',
        schedule_type: null,
        schedule_expr: null,
        schedule_time_of_day: null,
        timezone: 'Asia/Shanghai',
        window_selector: 'custom',
        window_params: {},
        params: { direction: 'both', export_daily: false, sort_order: 'desc', aggregate_all: false, batch_size: 200 },
        export_formats: ['csv'],
        output_filename_template: ''
      },
      editTask: null
    }
  },
  methods: {
    async checkHealth() {
      const res = await fetch('/api/health')
      const data = await res.json()
      this.health = data.status
    },
    async loadTasks() {
      const res = await apiFetch('/api/tasks', {}, this.apiKey)
      if (!res.ok) { alert('任务列表加载失败'); return }
      this.tasks = await res.json()
    },
    async createTask() {
      if (!this.validateTask(this.newTask)) return
      const res = await apiFetch('/api/tasks', { method: 'POST', body: JSON.stringify(this.newTask) }, this.apiKey)
      if (!res.ok) { alert('创建失败'); return }
      await this.loadTasks()
    },
    startEdit(t) {
      // 深拷贝并填充默认值
      const clone = JSON.parse(JSON.stringify(t))
      clone.kind = clone.kind || 'one_off'
      clone.window_selector = clone.window_selector || 'custom'
      clone.window_params = clone.window_params || {}
      clone.params = clone.params || { direction: 'both', export_daily: false, sort_order: 'desc', aggregate_all: false, batch_size: 200 }
      if (typeof clone.params.aggregate_all !== 'boolean') clone.params.aggregate_all = false
      if (!clone.params.batch_size) clone.params.batch_size = 200
      clone.export_formats = clone.export_formats && clone.export_formats.length ? clone.export_formats : ['csv']
      clone.timezone = clone.timezone || 'Asia/Shanghai'
      this.editTask = clone
    },
    cancelEdit() {
      this.editTask = null
    },
    async saveEdit() {
      if (!this.editTask || !this.editTask.id) return
      if (!this.validateTask(this.editTask)) return
      const allowKeys = ['name','active','kind','schedule_type','schedule_expr','schedule_time_of_day','timezone','window_selector','window_params','params','export_formats','output_filename_template']
      const body = {}
      for (const k of allowKeys) { if (k in this.editTask) body[k] = this.editTask[k] }
      const res = await apiFetch('/api/tasks/' + this.editTask.id, { method: 'PUT', body: JSON.stringify(body) }, this.apiKey)
      if (!res.ok) { const txt = await res.text(); alert('保存失败: ' + txt); return }
      this.editTask = null
      await this.loadTasks()
    },
    async removeTask(id) {
      if (!confirm('确认删除?')) return
      const res = await apiFetch('/api/tasks/' + id, { method: 'DELETE' }, this.apiKey)
      if (!res.ok) { alert('删除失败'); return }
      await this.loadTasks()
    },
    async runTask(id) {
      const res = await apiFetch('/api/tasks/' + id + '/run', { method: 'POST' }, this.apiKey)
      if (!res.ok) { alert('触发失败'); return }
      const data = await res.json(); alert('已触发: ' + data.job_id)
      await this.loadRuns()
    },
    async viewRuns(taskId) {
      const res = await apiFetch('/api/jobs?task_id=' + taskId, {}, this.apiKey)
      if (!res.ok) { alert('加载失败'); return }
      this.runs = await res.json()
    },
    async loadRuns() {
      const res = await apiFetch('/api/jobs', {}, this.apiKey)
      if (!res.ok) { alert('加载失败'); return }
      this.runs = await res.json()
    },
    formatDateTime(dt) {
      if (!dt) return '-'
      try { return new Date(dt).toLocaleString() } catch { return String(dt) }
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
      return sel || ''
    },
    renderTemplate(template, obj) {
      const params = obj.params || {}
      const province = params.province || 'province'
      const cp = params.cp || 'cp'
      const direction = params.direction || 'both'
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
               .replaceAll('{window}', windowLabel)
               .replaceAll('{date}', date)
      return out
    },
    filenamePreview(obj) {
      if (!obj) return ''
      const tpl = obj.output_filename_template || ''
      if (tpl) return this.renderTemplate(tpl, obj)
      // default naming
      const params = obj.params || {}
      const province = params.province || 'province'
      const cp = params.cp || 'cp'
      const direction = params.direction || 'both'
      const windowLabel = this.windowLabelForPreview(obj)
      return `${province}-${cp}-${direction}-${windowLabel}`
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
        const wp = obj.window_params || {}
        if (!wp.start_time || !wp.end_time) { alert('自定义时间范围需要填写开始与结束时间'); return false }
      }
      // batch_size 简单校验
      const ps = obj.params || {}
      if (ps.batch_size != null) {
        const n = Number(ps.batch_size)
        if (!Number.isFinite(n) || n < 10) { alert('batch_size 需为 >=10 的数字'); return false }
      }
      return true
    },
    async toggleActive(t) {
      const body = { active: !t.active }
      const res = await apiFetch('/api/tasks/' + t.id, { method: 'PUT', body: JSON.stringify(body) }, this.apiKey)
      if (!res.ok) { const txt = await res.text(); alert('切换失败: ' + txt); return }
      await this.loadTasks()
    },
    async deleteRun(id) {
      if (!confirm('确认删除该运行记录及产物？')) return
      try {
        const res = await apiFetch('/api/jobs/' + id, { method: 'DELETE' }, this.apiKey)
        const text = await res.text()
        if (!res.ok) {
          alert(`删除失败: ${res.status} ${text}`)
          return
        }
        await this.loadRuns()
      } catch (e) {
        console.error('删除请求异常', e)
        alert('删除请求异常：' + e)
      }
    }
  },
  mounted() {
    this.checkHealth()
    this.loadTasks()
    this.loadRuns()
  }
}).mount('#app')
