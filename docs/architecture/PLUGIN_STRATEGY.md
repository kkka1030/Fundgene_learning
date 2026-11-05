# FundGene 插件策略

更新日期：2026-04-29
状态：按当前项目阶段给出的最小必要插件决策。主干 MVP 闭环、Agent Runtime v2 deterministic spine 和首个 eval harness 已存在，下一阶段重点是 evidence/RAG 质量、观测和部署硬化。

## 1. 判断原则

- 只接入能直接提高当前阶段交付效率的插件
- 不为了“看起来完整”而堆平台
- 当前阶段以稳定主干、提升 agent runtime 证据质量、补观测和部署路径为目标，不以多平台运营为目标

## 2. 当前最小必要插件集

当前没有“非接不可”的插件。

如果必须从候选列表中只接一个，唯一值得优先考虑的是：

- `github`

前提：

- 先把仓库 git 边界问题修正到 FundGene 可独立管理
- 团队确实要用 issue / PR / review 驱动实施

原因：

- 当前最需要的是把 selective RAG、citation faithfulness、部署、观测、评测扩展和 bugfix 变成可追踪执行项
- 比起部署、支付、移动端、协作套件，GitHub 对当前阶段的收益最直接

## 3. 后面再接入

这些插件有价值，但不应阻塞当前阶段：

- `vercel`
  - 适合在 `apps/web` 持续可构建后提供前端预览与部署
- `sentry`
  - 适合在真实运行链路稳定后接入错误监控
- `figma`
  - 适合在需要多人视觉协作或设计交接时使用
- `linear`
  - 适合团队任务流明确后引入，不是当前单仓重建的先决条件

## 3.1 接入触发条件

- `github`
  - 仅在 FundGene 仓库边界已独立、远端协作流程明确后接入
- `vercel`
  - 仅在预览部署能直接服务演示、评审或用户测试后接入
- `sentry`
  - 仅在要开始长期运行或对外演示前接入
- `figma`
  - 仅在多人视觉协作成为真实瓶颈后接入
- `linear`
  - 仅在 issue 追踪已无法满足任务分工与节奏管理时接入

## 4. 当前不建议接入

以下候选与当前阶段不匹配，或收益显著低于维护成本：

- `huggingface`
- `slack`
- `box`
- `build ios apps`
- `build web apps`
- `canva`
- `cloudflare`
- `game studio`
- `gmail`
- `google calendar`
- `google drive`
- `jam`
- `netlify`
- `notion`
- `stripe`
- `test android apps`

原因归类：

- 当前没有移动端交付需求
- 当前没有支付需求
- 当前没有内容协作或办公套件刚需
- 当前没有必要同时维护多套前端部署平台
- 当前不应把产品重建工作变成插件集成工作

## 5. 结论

当前阶段的最优策略是：

- 插件接入极度克制
- 不让任何插件选择反向决定产品架构
- 先把主干产品、主干代码和主干文档做实，再逐步接入部署与监控能力
