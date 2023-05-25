import torch
from torch import nn
from torch.utils.data import DataLoader

def evaluate(model: nn.Module, data_loader: DataLoader, loss_fn):
    model.eval()
    total_loss = 0
    with torch.no_grad():
        for batch in data_loader:
            # Разделите данные на входные данные и целевые значения
            inputs, targets = batch

            # Проход вперед
            outputs = model(inputs)

            # Вычислите ошибку
            loss = loss_fn(outputs, targets)

            # Суммируйте ошибку
            total_loss += loss.item()

    # Верните среднюю ошибку
    return total_loss / len(data_loader)
