import torch
import torch.nn as nn
from kafka import KafkaConsumer
import torch.optim as optim

# 定义RNN模型
class RNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(RNN, self).__init__()
        self.hidden_size = hidden_size
        self.rnn = nn.RNN(input_size, hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(1, x.size(0), self.hidden_size)  # 初始化隐藏状态
        out, _ = self.rnn(x, h0)  # RNN前向传播
        out = self.fc(out[:, -1, :])  # 取最后一个时间步的输出
        return out

# 准备数据
def prepare_data(input_sequences, word_to_index):
    max_len = max(len(seq) for seq in input_sequences)
    input_tensor = torch.zeros(len(input_sequences), max_len, len(word_to_index))
    for i, seq in enumerate(input_sequences):
        for j, word in enumerate(seq):
            input_tensor[i, j, word_to_index[word]] = 1
    return input_tensor

# 从txt文件读取数据并进行清洗
def read_and_clean_data_from_txt(file_path, keywords):
    input_sequences = []
    labels = []
    with open(file_path, 'r') as file:
        for line in file:
            data = line.strip().split()
            if len(data) < 2:
                continue  # 忽略不完整的数据行
            # 检查序列中是否包含关键字，若包含则保留
            if any(word.lower() in keywords for word in data[:-1]):
                input_sequences.append(data[:-1])
                if data[-1].isdigit():  # 检查最后一个元素是否是整数，作为标签
                    labels.append(int(data[-1]))
                else:
                    # 处理非整数标签
                    pass
    return input_sequences, labels
import os

# 指定目录路径
directory_path = '/content/sample_data/data'

# 列出目录中的所有文件
files = os.listdir(directory_path)
# 示例数据
#file_path = '/content/sample_data/data/marked_data2.txt'  # 修改为你的文件路径
keywords = {"rrcconnectionreconfiguration", "rrcconnectionreconfigurationcomplete",
            "ulinformationtransfer", "rrcconnectionrequest", "rrcconnectionsetup", "rrcconnectionsetupcomplete",
            "dlinformation transfer", "ulinformation transfer", "securitymodecommand",
            "securitymodecomplete", "uecapabilityenquiry", "uecapabilityinformation"}
for file_path in files:
  input_sequences, labels = read_and_clean_data_from_txt("/content/sample_data/data/"+file_path, keywords)

# 构建词汇表和标签
unique_words = set(word for seq in input_sequences for word in seq)
word_to_index = {word: i for i, word in enumerate(unique_words)}

# 训练模型
def train_model(model, input_sequences, labels, word_to_index, hidden_size, output_size, num_epochs=100):
    model.train()
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    input_tensor = prepare_data(input_sequences, word_to_index)
    labels_tensor = torch.tensor(labels, dtype=torch.long)  # 使用从文件中读取的标签
    for epoch in range(num_epochs):
        optimizer.zero_grad()
        output = model(input_tensor)
        loss = criterion(output, labels_tensor)
        loss.backward()
        optimizer.step()
        if epoch % 10 == 0:
            print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.4f}')

hidden_size = 16  # 隐藏层大小
output_size = 2  # 输出大小，二分类

# 创建和训练模型
model = RNN(len(unique_words), hidden_size, output_size)
train_model(model, input_sequences, labels, word_to_index, hidden_size, output_size, num_epochs=100)
# 加载模型
model_path = 'rnn_model.pth'
model = RNN(len(unique_words), hidden_size, output_size)
model.load_state_dict(torch.load(model_path))
model.eval()

# 准备输入数据的函数
def prepare_input_data(input_sequences, word_to_index):
    max_len = max(len(seq) for seq in input_sequences)
    input_tensor = torch.zeros(len(input_sequences), max_len, len(word_to_index))
    for i, seq in enumerate(input_sequences):
        for j, word_index in enumerate(seq):
            input_tensor[i, j, word_index] = 1
    return input_tensor

# Kafka consumer 配置
bootstrap_servers = 'localhost:9092'
topic = 'nas-topic'

# 创建 Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', enable_auto_commit=True)

# 处理输入数据并进行推理
for message in consumer:
    input_data = message.value.decode('utf-8').strip().split()
    input_sequences = [[word_to_index[word.lower()] for word in input_data if word.lower() in word_to_index]]

    # 准备输入数据
    input_tensor = prepare_input_data(input_sequences, word_to_index)

    # 执行推理
    with torch.no_grad():
        output = model(input_tensor)

    # 处理输出
    predicted_labels = torch.argmax(output, dim=1)
    for label in predicted_labels:
        if label.item() == 1:
            print("Out of order")
        else:
            print("In order")

                                                
