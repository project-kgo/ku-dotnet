# Ku.Utils

Ku.Utils 是一个基于 .NET 10 的通用工具库，面向其他 .NET 项目引用。

## 项目结构

```text
src/Ku.Utils/              # 主类库
tests/Ku.Utils.Tests/      # 单元测试
docs/                      # 项目文档
```

## 开发命令

```bash
dotnet restore
dotnet build Ku.Utils.sln
dotnet test Ku.Utils.sln
```

## 约定

- 目标框架：`net10.0`
- 根命名空间：`Ku.Utils`
- 测试框架：xUnit
- 包版本管理：Central Package Management
