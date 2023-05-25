import torch
from torch import nn
from torch.utils.data import DataLoader
from torch.optim import Optimizer

def train(model: nn.Module, data_loader: DataLoader, optimizer: Optimizer, loss_fn):
    model.train()
    total_loss = 0
    for batch in data_loader:
        # Разделите данные на входные данные и целевые значения
        inputs, targets = batch

        # Обнулите градиенты
        optimizer.zero_grad()

        # Проход вперед
        outputs = model(inputs)

        # Вычислите ошибку
        loss = loss_fn(outputs, targets)

        # Проход назад
        loss.backward()

        # Шаг оптимизации
        optimizer.step()

        # Напечатайте ошибку на текущем батче
        print(f'{loss.item():.5f}')

        # Суммируйте ошибку
        total_loss += loss.item()

    # Верните среднюю ошибку
    return total_loss / len(data_loader)
