import pandas as pd
import json
import os
import time
from tqdm import tqdm
from openai import OpenAI
import concurrent.futures
from typing import List, Dict
import random
import threading
from concurrent.futures import ThreadPoolExecutor

# API密钥列表
API_KEYS = [
    "sk-txdepjdzhjjyweyykjochtdlmiwkgcvftzwtlbfmzklhmrbb",
    "sk-dvhxkthitsisoucqhotdogwkdqyaarphukmbcsvltmijmxsc",
    "sk-vssadxykuuijjumuzstmmjtxnclydfsftwtijnbknofnxiog",
    "sk-humojulqcvrntpkskfizxcvuysybuhixdadcubfasjjpxqom",
    "sk-lbhazfhbjxaeocrkljsbnnnmgyvgtrdyyzhsbzauwsubroxg",
    "sk-oqgxsxzzarscncbkxemkqjhlvfkxkruzhwzrkhdjozjhqibb",
    "sk-mtdvnzzyymxhhcmwlwieruduibjwuxavukconzkrieznqpew",
    "sk-pjsyxpghnfqqkmrpdffaaiwttjkyjszyazxpconaqljpzjpr",
    "sk-swgvyqhuizilbovfyyjczquwziuxijuuzsfoojxkpxpghgcu",
    "sk-kthfsbxyqrmrpsxbxykmhgmwhaeulvsiqyfdlguehwocrpyn",
    "sk-xeefluzqiavvqccqezglyhbymokwvomnlphegqjqhixqfgcs",
    "sk-msldiujrxuimjfebxqdxjythrnbdacorvzeexfjigetgpegu",
    "sk-ofzzdcwanwmgwgrkacrunsecgsdkrwwfgnxrdvqlyszfoofq",
    "sk-dkvxlkjshbqvfohmxpbcporyiwryajkitaxnnfkemeqqxmsm",
    "sk-xtfmftvpgqsrlgyefydxwqvajxlugivpsyutjxpynznbkqql",
    "sk-amkaczzjmalzdryvulduapzkaqxcdslpiemadwpmhealjtss",
    "sk-gydwgkbmnimosiydeuhbisvefxyvgbxkgxtmciywwmiaqwvj",
    "sk-uyivdgutlqlpbknqcqqhwzmztpzweqzelqfnubppclroxlpk",
    "sk-hsdfjzxfqyeidujuvbtomscryoqdhjbcfkjxvqagamrnogah",
    "sk-rjwbovupuurbaepkfajaqsrvmqthjkpfhcrtuhmtetbdghgp",
    "sk-drgpidostmhmqblytrsfctnptaqjhjwfnpukjtbvlcbsulfz",
    "sk-wbbypmdpmdnulcomylncqgxkjvhbqglhstuysjibezmlidja",
    "sk-bdiarzxabfbvwbvyoetygcktrdxgzawrhdnofjhryxrrqfrz",
    "sk-qsdxtvreubgnijlgmlnqzihvkjoaoczbmktgydnltsphkngv",
    "sk-prakzkvoppfihzcowxewuowuerqullcwmzhuhjcxuikkplki",
    "sk-ihgserckvkcxwmemvtqdvkztsqtvhnnuttmzyihzdcfphjeq",
    "sk-ugtgfnqobdcpqcgvhafivboxzhewrnfjidqonvulylnxqayc",
    "sk-ffbxtcdbgeuikqaqagrssdgzgikfpuhdclanvnubwrcbgbns",
    "sk-kuselbrvczuukrdfgjwoxnejwujazybhkaqdjdynofyyphnu",
    "sk-yowbhfgkoaqilgezgywubevvwhojbtboxeentxvkjdeeexuf",
    "sk-nzkhptgpsmfejgqnkezlxhtdpkjwyktuzrjjtkesjjvestfg",
    "sk-inzrcpzvupcavpgzkvxfogfqjepefyeoedvivebvezivzbhk",
    "sk-hbxujhhddosslfzkxblzxpqstwdaliqxfhvikoevpbjxbmkf",
    "sk-milsnutlhoshfecynmhyxfxtobwmnxlmntpkxwxxydavgqtq",
    "sk-amzoedctdgzfexextgemccmmzqthfkgyjnceomdvafqclvid",
    "sk-wchjwwdadyoltbsgoxfgylbdwrnxewtmbxckayeoxxndflxt",
    "sk-rujnvkgbdckhlkhtumtexljpwehtssuzzlnykebcwdhrwmha",
    "sk-hkzrjuesgnwjeqexauhaeuffksxlqeutwvenaefhkebqegdw",
    "sk-bzqiwcexeyyizkjvdmurutdewjlofkpqkdhksuxbwdqwkdfq",
    "sk-mhzuhbiiafbgwenlxkkpymbciyvwywnvnhzbubhipytkwucs",
    "sk-nemjwhmpmjswpwshyivkwqclqoatmyohwunglvrnryyhvyhr",
    "sk-mpnfceqitccxziryldvsnolepzpxouocisyquqbppgbigmkg",
    "sk-mevmmtsknbsxwlkhmxqgunauoxmpecaviqmhyqhrxbxrrpyb",
    "sk-pkinzdrjtfqblndvecgpexlbyocrywxqnlbragtisyvggzwb",
    "sk-iyqbmtwehrmupayxzmjcjlwdphxdlisyfvaqvdusqtxmnewy",
    "sk-eclqfrmlrxhksczcztcxsqddzewocqqssgpfytoeuxnapqyd",
    "sk-xlqzqzmuwvjmlfjeptnoxigfqdjteubfpwodfjascwvcjpfe",
    "sk-hxnpujinwqzsgroqsqkqhrvgrgvzojhrjlnmazumklroseto",
    "sk-iaugvqvzlgqvfzuhvaphqplsqjlpxgqsjdxviaozpbbgcuea",
    "sk-iylyojmpoegsgafrhlhwagldlwrvbrdnqicbqybefvnvinrn"
]

# 输入输出路径
input_excel = r"C:\Users\Administrator\Desktop\门诊记录.xlsx"
output_dir = r"C:\Users\Administrator\Desktop\结构化病例"
output_json = os.path.join(output_dir, "structured_cases.json")

os.makedirs(output_dir, exist_ok=True)

# 全局变量
processed_count = 0
failed_count = 0
key_failure_counts = {key: 0 for key in API_KEYS}
disabled_keys = set()
result_lock = threading.Lock()

def structure_case(case_data, api_key):
    """使用API将病例数据结构化"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            cleaned_data = {k: (str(v) if v is not None else "") for k, v in case_data.items()}
            
            prompt = f"""
请将以下门诊记录信息整理成一段连贯的病例描述。包含所有重要的临床信息,使用专业但易懂的语言。

病人信息：
"""
            for key, value in cleaned_data.items():
                if value and value != "nan":
                    prompt += f"{key}: {value}\n"
            
            prompt += "\n请用一段话描述这个病例。"
            
            client = OpenAI(
                api_key=api_key,
                base_url="https://api.siliconflow.cn/v1"
            )
            
            response = client.chat.completions.create(
                model="deepseek-ai/DeepSeek-V2.5",
                messages=[
                    {"role": "system", "content": "你是一个专业的医生,负责整理病例信息。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            retry_count += 1
            print(f"Attempt {retry_count} failed for key {api_key[:8]}...: {e}")
            if retry_count < max_retries:
                time.sleep(2)  # 重试前等待
            
    return None

def process_cases():
    """处理所有病例数据"""
    try:
        df = pd.read_excel(input_excel)
        print(f"Loaded {len(df)} cases from Excel")
        print("Column names:", list(df.columns))
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # 将数据分成多个批次，每个API key处理一部分
    available_keys = API_KEYS.copy()
    batch_size = len(df) // len(available_keys) + 1
    batches = []
    
    for i in range(0, len(df), batch_size):
        end = min(i + batch_size, len(df))
        batches.append((df.iloc[i:end], available_keys[i // batch_size]))
    
    structured_cases = []
    
    def process_batch(batch_data, api_key):
        batch_results = []
        for idx, row in batch_data.iterrows():
            try:
                case_data = row.to_dict()
                structured_text = structure_case(case_data, api_key)
                if structured_text:
                    case_result = {
                        "case_id": idx + 1,
                        "structured_description": structured_text,
                        "raw_data": {k: str(v) for k, v in case_data.items() if pd.notna(v)}
                    }
                    batch_results.append(case_result)
                    
            except Exception as e:
                print(f"Error processing case {idx + 1} with key {api_key[:8]}...: {e}")
                
        return batch_results
    
    with ThreadPoolExecutor(max_workers=len(available_keys)) as executor:
        futures = []
        with tqdm(total=len(df)) as pbar:
            # 提交所有批次的任务
            for batch_data, api_key in batches:
                future = executor.submit(process_batch, batch_data, api_key)
                futures.append((future, len(batch_data)))
            
            # 处理完成的结果
            for future, batch_len in futures:
                try:
                    results = future.result()
                    with result_lock:
                        structured_cases.extend(results)
                        # 每累积5个结果保存一次
                        if len(structured_cases) % 5 == 0:
                            save_results(structured_cases)
                    pbar.update(batch_len)
                except Exception as e:
                    print(f"Batch processing failed: {e}")
    
    # 最终保存
    save_results(structured_cases)
    print(f"\nProcessed {len(structured_cases)} cases successfully")

def save_results(cases):
    """保存结果到JSON文件"""
    try:
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(cases, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving results: {e}")

if __name__ == "__main__":
    process_cases()
