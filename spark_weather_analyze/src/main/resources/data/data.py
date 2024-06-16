import pandas as pd
import os

# CSV文件所在目录
directory = 'E:\\Dasan\\bigdata\\data\\2024'

# 获取目录中所有CSV文件
csv_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv')]

# 读取第一个CSV文件
first_file = csv_files[0]
combined_df = pd.read_csv(first_file)

# 读取并合并其余的CSV文件（不包括表头）
for file in csv_files[1:]:
    df = pd.read_csv(file)
    combined_df = pd.concat([combined_df, df], ignore_index=True)

# 保存合并后的数据到新的CSV文件
combined_df.to_csv('combined_file.csv', index=False)
