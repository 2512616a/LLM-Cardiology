import json
import os
import time
from tqdm import tqdm
from openai import OpenAI
import concurrent.futures
from typing import List, Dict
import random
import threading

# API密钥列表
API_KEYS = [
    "sk-bnqjbyzdqxnoyvnyymyykxihhksnadiqojpfkbsepmuyhygm",
    "sk-tgqpwijyvwpmvyjznxfjtpktiacnwwxgwioelenrhjhhpqiq",
    "sk-prqactqdidqbrbquxlkgkkxfhzenacdsuaaeykyvlwlftjvn",
    "sk-tclgrvzeenegousrhpbdorhevwdwdnmnnejrbsqvmvhcnkgl",
    "sk-pfimfvyawbcteuwdseapivbucrpjllcyricwvloxmxcnecvo",
    "sk-fqfgyrjlikdivvaoofmtleybuenhegidnhivpefxqdrqemtz",
    "sk-atrpuygzftgjkubwyrcpqrlpwngxcmdatninkhblfnflbntq",
    "sk-wqfkiprkvybuwsghwmugkogauxjqzejwtgdqkpdqojbavhef",
    "sk-ngvmdcveeqermfuwbskdogijdoftecxbncmhwrabahmgdsim",
    "sk-ljtyfivhtpztjyyibzydnwybbziwltkntcwbcqhsmuyaqwbz",
    "sk-kawxmfrwgbbujfurrqrwglusbuuimqnssvbrpjmvoulecnxz",
    "sk-hizdfkiueamqmdvmeqrgxxnbtepdiqwvqufzhaiikpwhvvkr",
    "sk-qelftpofjbehueaqkcyfbuhkwcyyiwmdjllymbofnbfvyotd",
    "sk-dcsngzomzwrfewvaywriichvdjyxdkmqklbanoqgscmqfsml",
    "sk-dklfxgrgnmygiqbezydjlarxxhpurccxaxnqjyoizzmjvazi",
    "sk-xcqkiysmfegxjzvajjwnarugosultdjuszcjzxfqfkxkqgav",
    "sk-lfgtuxfyezvwdwcsigqexlrgtrwatwdlfhmtfnbqzsfgqual",
    "sk-ggqcctrzykjiicmiooytgunkrseixjtwwlczrqabjioinhbu",
    "sk-mbhffgkpbwpdqkyaacixnmpkyzkgapfcvphhzqckbtfdqfrv",
    "sk-fuuouepdbifqdyrctjqdmogeqvbetyomsivubrdyvkocoszd",
    "sk-emrhguqyfiuswiztaxzhvjfotxyznneafvxgltkfzlfgxhcg",
    # 新增的API密钥
    "sk-uqqhpoxqskzvrxnnujuhsrlunwzdeijhvyjumxkzqkubmtrq",
    "sk-rjivgspvfhlvjhverrxpibahphjomhpbborkhqbaxiycuvpo",
    "sk-cejvlphrpurpuhubcjlaymoecnpmoszmxfgwfcawfwbupqgx",
    "sk-mzrdyvfepqfdbycrzgthacgrpcsooncbxjpavquhfbijnhma",
    "sk-zzqrprwnxjgyfqekdosvurounveznqcwoctuqrljylftsegs",
    "sk-ugwoaihkzjrwzvpgcnfmdmqhjumnxzzxpcwhytewqdsseqbs",
    "sk-iqcmcydevhxgsxoyywjpdolfoclzsrfzcigyoxuhptatgrgd",
    "sk-xindvhsnwbikzyggxgbczbokkndwqwjpiitsrucdjqwqdjbk",
    "sk-ocnsyffnlffthgyttvnmzvdcqhvgshzlgnbelqshvmkczely",
    "sk-mveovhhjxnhyknkqhefgbuwjncvqldfoiueuwppnpamcoajp",
    "sk-mrkmpsiorurmkpthelcsvpopnzdmexxgevmqqftlabqynqey",
    "sk-fpczhcmfhkzrddjwtftxqkfsbqqdfazetvadencpotxtflyz",
    "sk-prvgnyjcruhgihzzsnilwqmjtqegffuuzijydwivjgtruidg",
    "sk-eabkilpcadqcgmqampgdavzvgnyglaubxghtzifocrbrosaq",
    "sk-gmmmxbkkhugzctxssuvybglqojmmeqqdnihbsvolbjxonnhp",
    "sk-xavexnvlzyvvjwrhofdwbtnunktmrokwxrjslbxxxfplnyxd",
    "sk-zdtiugrlewbordtmbgarjqrdurdzseebxuqgmjguslewcmmh",
    "sk-hubcphqeqrgpxfgfzleggnzsorivuidpycxspsadfjquzjoy",
    "sk-jwqatmlohdkutligeyrjxxdkizhfhdoxnmkjmlpwxeqtptjg",
    "sk-zygfuptpslsgtdbenyeiarmojklrkkxkpbjihqpqzzonqowx",
    "sk-yrznntuoibqhmfubarsjaocnmuroyrioifhicpnvldqzllnt",
    "sk-lkaasuvqqoxvmmufjnjevxvbvgqcygcnxfwrujhbhlxssxba",
    "sk-kjdmecaaeiuyfddqlbbetnbbhqayrbfgutmgvdbtjqekoxsw",
    "sk-wrjaxlstdarthgtgdhdnwqptbgpdyfogvjjzxedjgzxvcuex",
    "sk-dfylwospxufbshhxaitdhazmzmpaqdxbapewzulakcmqqlwp",
    "sk-eigaeilckckddqgoffxhpbxnbzrphobtyrvizpbrvjvvtsrv"
]

# 输入和输出路径
input_json_path = r"D:\try\13_Heart\Data\medical_o1_verifiable_problem.json"
output_dir = r"D:\try\13_Heart\Data\o1_ver"
output_json_path = os.path.join(output_dir, "heart_related_questions.json")
temp_output_dir = output_dir
temp_output_prefix = "temp_heart_related_"

# 确保输出目录存在
os.makedirs(output_dir, exist_ok=True)

# 添加heart.json路径
heart_json_path = os.path.join(output_dir, "heart.json")

# 心内科相关性判断的prompt
cardiology_prompt = """
Please determine if the following question is related to cardiology. Cardiology mainly involves diseases of the heart and vascular system, including but not limited to:
- Coronary artery disease, myocardial infarction, angina
- Heart failure
- Arrhythmias
- Hypertension and its cardiac complications
- Cardiomyopathy, myocarditis
- Heart valve diseases
- Congenital heart disease
- Pericardial diseases
- Aortic and peripheral vascular diseases
- Venous thrombosis and embolism
- Pulmonary hypertension
- Heart transplant related issues

Question: {question}
Please answer in JSON format: {{"is_cardiology_related": true/false}}
"""

# 线程锁，用于保护共享资源
result_lock = threading.Lock()
key_locks = {key: threading.Lock() for key in API_KEYS}
heart_related_data = []

# 在文件开头添加一个全局计数器
processed_count = 0
failed_count = 0
related_count = 0
not_related_count = 0
processed_lock = threading.Lock()

# 在文件开头添加新的全局变量
key_failure_counts = {key: 0 for key in API_KEYS}
disabled_keys = set()
failed_questions = []

def save_temp_results(batch_results, batch_id):
    """保存临时结果到JSON文件"""
    if not batch_results:
        return
    
    temp_file_path = os.path.join(temp_output_dir, f"{temp_output_prefix}{batch_id}.json")
    try:
        with result_lock:
            with open(temp_file_path, 'w', encoding='utf-8') as f:
                json.dump(batch_results, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(batch_results)} items to temporary file: {temp_file_path}")
    except Exception as e:
        print(f"Error saving temporary results: {e}")

def is_cardiology_related(question, api_key):
    """判断问题是否与心内科相关"""
    global key_failure_counts, disabled_keys
    max_retries = 3
    retry_count = 0
    last_error = None  # 添加变量保存最后一次错误
    
    # 如果key已被禁用，尝试使用其他可用的key
    if api_key in disabled_keys:
        available_keys = [k for k in API_KEYS if k not in disabled_keys]
        if not available_keys:
            print("No available API keys remaining!")
            return False
        api_key = random.choice(available_keys)
    
    while retry_count < max_retries:
        try:
            with key_locks[api_key]:
                client = OpenAI(
                    api_key=api_key,
                    base_url="https://api.siliconflow.cn/v1"
                )
                
                response = client.chat.completions.create(
                    model="deepseek-ai/DeepSeek-V2.5",
                    messages=[
                        {"role": "system", "content": "You are a professional cardiologist who needs to determine if questions are cardiology-related. Please output only in JSON format."},
                        {"role": "user", "content": cardiology_prompt.format(question=question)}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1
                )
                
                result = json.loads(response.choices[0].message.content)
                # 成功调用后重置失败计数
                key_failure_counts[api_key] = 0
                return result.get("is_cardiology_related", False)
                
        except Exception as e:
            last_error = e  # 保存错误信息
            retry_count += 1
            key_failure_counts[api_key] += 1
            
            # 检查是否应该禁用这个key
            if key_failure_counts[api_key] >= 3:
                print(f"Disabling API key {api_key[:8]}... due to multiple failures")
                disabled_keys.add(api_key)
                # 尝试使用其他key
                available_keys = [k for k in API_KEYS if k not in disabled_keys]
                if available_keys:
                    api_key = random.choice(available_keys)
                    print(f"Switching to new API key: {api_key[:8]}...")
                else:
                    print("No available API keys remaining!")
                    return False
            
            error_msg = str(e).lower()
            if "rate limit" in error_msg or "too many requests" in error_msg:
                time.sleep(60)
            else:
                time.sleep(5)
    
    # 如果处理失败，添加到失败列表
    if last_error:  # 使用保存的错误信息
        failed_questions.append({
            "question": question, 
            "error": str(last_error),
            "failed_key": api_key
        })
    return False

def save_to_heart_json(item):
    """实时保存心脏相关问题到heart.json"""
    try:
        with result_lock:
            # 读取现有数据
            existing_data = []
            if os.path.exists(heart_json_path):
                with open(heart_json_path, 'r', encoding='utf-8') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []
            
            # 检查重复
            question = item.get("Open-ended Verifiable Question", "")
            if not any(q.get("Open-ended Verifiable Question") == question for q in existing_data):
                existing_data.append(item)
                
                # 保存更新后的数据
                with open(heart_json_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
                print(f"Added new heart-related question to {heart_json_path}")
    except Exception as e:
        print(f"Error saving to heart.json: {e}")

def process_item(item, api_key, batch_id):
    """处理单个问题"""
    global processed_count, failed_count, related_count, not_related_count
    try:
        question = item.get("Open-ended Verifiable Question", "")
        if not question:
            with processed_lock:
                processed_count += 1
                # 更新统计信息并输出
                update_and_print_stats(len(data))
            return None
        
        try:
            is_related = is_cardiology_related(question, api_key)
            
            with processed_lock:
                processed_count += 1
                if is_related:
                    related_count += 1
                else:
                    not_related_count += 1
                # 更新统计信息并输出
                update_and_print_stats(len(data))
            
            if is_related:
                print(f"[Key: {api_key[:8]}...] Heart related: {question[:50]}...")
                # 立即保存到heart.json
                save_to_heart_json(item)
                return item
            else:
                print(f"[Key: {api_key[:8]}...] Not related (skipped): {question[:50]}...")
                return None
        except Exception as e:
            with processed_lock:
                processed_count += 1
                failed_count += 1
                # 更新统计信息并输出
                update_and_print_stats(len(data))
            print(f"Error processing question content with key {api_key[:8]}...: {e}")
            return None
            
    except Exception as e:
        print(f"Error processing question item with key {api_key[:8]}...: {e}")
        with processed_lock:
            processed_count += 1
            failed_count += 1
            # 更新统计信息并输出
            update_and_print_stats(len(data))
        return None

def process_batch(batch, api_key, batch_id, pbar):
    """处理一批问题"""
    batch_results = []
    
    for item in batch:
        result = process_item(item, api_key, batch_id)
        if result:
            with result_lock:
                heart_related_data.append(result)
                batch_results.append(result)
                pbar.set_postfix({'Found': len(heart_related_data)}, refresh=True)
        
        pbar.update(1)
        
        # 每处理5个问题保存一次临时结果
        if len(batch_results) > 0 and len(batch_results) % 5 == 0:
            save_temp_results(batch_results, f"{batch_id}_{len(batch_results)}")
    
    # 批次处理完成后保存一次
    if batch_results:
        save_temp_results(batch_results, batch_id)
    
    return batch_results

def update_and_print_stats(total_questions):
    """更新并输出统计信息"""
    remaining = total_questions - processed_count
    # 使用\r来覆盖同一行，这样不会产生大量滚动输出
    print(f"\r已处理: {processed_count} | 成功: {processed_count-failed_count} | 失败: {failed_count} | 相关: {related_count} | 不相关: {not_related_count} | 剩余: {remaining} | 总共: {total_questions}", end="", flush=True)

def save_failed_questions():
    """保存处理失败的问题到JSON文件"""
    failed_questions_path = os.path.join(output_dir, "failed_questions.json")
    try:
        with open(failed_questions_path, 'w', encoding='utf-8') as f:
            json.dump(failed_questions, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(failed_questions)} failed questions to {failed_questions_path}")
    except Exception as e:
        print(f"Error saving failed questions: {e}")

def process_json_file():
    """处理JSON文件，使用多线程和多API密钥筛选心内科相关问题"""
    global data  # 将data设为全局变量，以便在process_item中访问总数
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return
    
    # 为每个API密钥创建10个线程
    threads_per_key = 10
    total_threads = len(API_KEYS) * threads_per_key
    
    # 将数据分成小批次
    batch_size = (len(data) // total_threads) + 1
    batches = []
    
    for i in range(0, len(data), batch_size):
        end = min(i + batch_size, len(data))
        batches.append((data[i:end], i // batch_size))
    
    # 使用线程池并发处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=total_threads) as executor:
        futures = []
        
        # 使用tqdm显示总体进度
        with tqdm(total=len(data), desc="Processing questions") as pbar:
            # 为每个API密钥分配批次
            for key_idx, api_key in enumerate(API_KEYS):
                key_batches = batches[key_idx::len(API_KEYS)]  # 交错分配批次
                
                for thread_idx in range(threads_per_key):
                    thread_batches = key_batches[thread_idx::threads_per_key]  # 进一步交错分配
                    
                    for batch, batch_id in thread_batches:
                        unique_batch_id = f"key{key_idx}_thread{thread_idx}_batch{batch_id}"
                        futures.append(executor.submit(process_batch, batch, api_key, unique_batch_id, pbar))
            
            # 等待所有任务完成
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Batch processing failed: {e}")

    # 合并所有临时文件的结果
    all_results = []
    temp_files = [f for f in os.listdir(temp_output_dir) if f.startswith(temp_output_prefix)]
    
    for temp_file in temp_files:
        try:
            with open(os.path.join(temp_output_dir, temp_file), 'r', encoding='utf-8') as f:
                temp_data = json.load(f)
                all_results.extend(temp_data)
        except Exception as e:
            print(f"Error reading temporary file {temp_file}: {e}")
    
    # 去重
    unique_results = []
    seen_questions = set()
    
    for item in all_results:
        question = item.get("Open-ended Verifiable Question", "")
        if question and question not in seen_questions:
            seen_questions.add(question)
            unique_results.append(item)
    
    # 保存最终结果
    try:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(unique_results, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(unique_results)} unique cardiology-related questions to {output_json_path}")
    except Exception as e:
        print(f"Error saving final results: {e}")
    
    # 清理临时文件
    for temp_file in temp_files:
        try:
            os.remove(os.path.join(temp_output_dir, temp_file))
        except Exception as e:
            print(f"Error removing temporary file {temp_file}: {e}")

    # 最后打印一次统计信息，并添加换行符
    print("\n")
    print(f"Total processed questions: {processed_count}")
    print(f"Successfully processed: {processed_count-failed_count}")
    print(f"Failed to process: {failed_count}")
    print(f"Heart-related questions: {related_count}")
    print(f"Non-related questions: {not_related_count}")
    print(f"Total input questions: {len(data)}")

    # 在函数结束前保存失败的问题
    save_failed_questions()
    
    # 打印API key使用情况
    print("\nAPI Key Statistics:")
    for key in API_KEYS:
        status = "Disabled" if key in disabled_keys else "Active"
        print(f"Key {key[:8]}...: {status} (Failed {key_failure_counts[key]} times)")

if __name__ == "__main__":
    process_json_file()
