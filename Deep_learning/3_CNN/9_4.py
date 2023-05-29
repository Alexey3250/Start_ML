import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms
from torch.utils.data import DataLoader
import torchvision
from tqdm import tqdm



def create_mlp_model():
    model = nn.Sequential(
        nn.Flatten(),
        nn.Linear(28*28, 512),
        nn.ReLU(),
        nn.Linear(512, 256),
        nn.ReLU(),
        nn.Linear(256, 128),
        nn.ReLU(),
        nn.Linear(128, 64),
        nn.ReLU(),
        nn.Linear(64, 10)
    )
    return model



device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def train(model: nn.Module, device: torch.device, data_loader: DataLoader, optimizer: torch.optim.Optimizer, loss_fn):
    model.train()
    total_loss = 0
    total = 0
    correct = 0
    for x, y in tqdm(data_loader, desc='Train'):
        x = x.to(device)
        y = y.to(device)
        optimizer.zero_grad()
        output = model(x)
        loss = loss_fn(output, y)
        loss.backward()
        total_loss += loss.item()
        optimizer.step()
        _, y_pred = torch.max(output, 1)
        total += y.size(0)
        correct += (y_pred == y).sum().item()
    return total_loss / len(data_loader), correct / total

def evaluate(model: nn.Module, device: torch.device, data_loader: DataLoader, loss_fn):
    model.eval()
    total_loss = 0
    total = 0
    correct = 0
    for x, y in tqdm(data_loader, desc='Evaluate'):
        x = x.to(device)
        y = y.to(device)
        output = model(x)
        loss = loss_fn(output, y)
        total_loss += loss.item()
        _, y_pred = torch.max(output, 1)
        total += y.size(0)
        correct += (y_pred == y).sum().item()
    return total_loss / len(data_loader), correct / total

model = create_mlp_model().to(device)
loss_fn = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

transform = torchvision.transforms.Compose([torchvision.transforms.ToTensor(), torchvision.transforms.Normalize((0.5,), (0.5,))])

trainset = torchvision.datasets.MNIST('~/.pytorch/MNIST_data/', download=True, train=True, transform=transform)
trainloader = torch.utils.data.DataLoader(trainset, batch_size=64, shuffle=True)
# batch size is ho

testset = torchvision.datasets.MNIST('~/.pytorch/MNIST_data/', download=True, train=False, transform=transform)
testloader = torch.utils.data.DataLoader(testset, batch_size=64, shuffle=True)

for epoch in range(100):
    train_loss, train_accuracy = train(model, device, trainloader, optimizer, loss_fn)
    print(f'Epoch {epoch+1}, Train Loss: {train_loss}, Train Accuracy: {train_accuracy}')
    test_loss, test_accuracy = evaluate(model, device, testloader, loss_fn)
    print(f'Epoch {epoch+1}, Test Loss: {test_loss}, Test Accuracy: {test_accuracy}')


torch.save(model.state_dict(), 'mnist_model.pth')