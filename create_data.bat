@echo off
REM 设置参数
set DATA_PATH=D:\try\13_Heart\Data\format_data.json
set MODEL_NAME=deepseek-ai/DeepSeek-V3
set API_KEY=sk-axcmjwcwfeigofmpqbqjghoczpknqfklxeewdebezshrpmmm
set API_URL=https://api.siliconflow.cn/v1/chat/completions
set MAX_SEARCH_ATTEMPTS=1
set MAX_SEARCH_DEPTH=2
set NUM_PROCESS=5
set LIMIT_NUM=10
set OUTPUT_DIR=D:\try\13_Heart\Data

REM 设置Python使用UTF-8编码
set PYTHONIOENCODING=utf-8

REM 创建输出目录（如果不存在）
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

REM 运行Python脚本
python Code\create_chinese.py ^
  --data_path=%DATA_PATH% ^
  --model_name=%MODEL_NAME% ^
  --api_key=%API_KEY% ^
  --api_url=%API_URL% ^
  --max_search_attempts=%MAX_SEARCH_ATTEMPTS% ^
  --max_search_depth=%MAX_SEARCH_DEPTH% ^
  --num_process=%NUM_PROCESS% ^
  --limit_num=%LIMIT_NUM%

REM 移动生成的JSON文件到输出目录
move *_CoT_search_*.json "%OUTPUT_DIR%\"

echo 处理完成！输出文件已保存到 %OUTPUT_DIR%
pause