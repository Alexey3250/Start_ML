import torch.nn as nn
import torch.nn.functional as F

def create_advanced_conv_cifar():
    class Net(nn.Module):
        def __init__(self):
            super(Net, self).__init__()
            self.conv1 = nn.Conv2d(3, 128, 5, padding=2)
            self.conv2 = nn.Conv2d(128, 128, 5, padding=2)
            self.conv3 = nn.Conv2d(128, 256, 3, padding=1)
            self.conv4 = nn.Conv2d(256, 256, 3, padding=1)
            self.pool = nn.MaxPool2d(2, 2)
            self.bn_conv1 = nn.BatchNorm2d(128)
            self.bn_conv2 = nn.BatchNorm2d(128)
            self.bn_conv3 = nn.BatchNorm2d(256)
            self.bn_conv4 = nn.BatchNorm2d(256)
            self.bn_dense1 = nn.BatchNorm1d(1024)
            self.bn_dense2 = nn.BatchNorm1d(512)
            self.dropout_conv = nn.Dropout2d(p=0.25)
            self.dropout = nn.Dropout(p=0.5)
            self.fc1 = nn.Linear(256 * 8 * 8, 1024)
            self.fc2 = nn.Linear(1024, 512)
            self.fc3 = nn.Linear(512, 10)

        def conv_layers(self, x):
            out = F.relu(self.bn_conv1(self.conv1(x)))
            out = F.relu(self.bn_conv2(self.conv2(out)))
            out = self.pool(out)
            out = self.dropout_conv(out)
            out = F.relu(self.bn_conv3(self.conv3(out)))
            out = F.relu(self.bn_conv4(self.conv4(out)))
            out = self.pool(out)
            out = self.dropout_conv(out)
            return out

        def dense_layers(self, x):
            out = F.relu(self.bn_dense1(self.fc1(x)))
            out = self.dropout(out)
            out = F.relu(self.bn_dense2(self.fc2(out)))
            out = self.dropout(out)
            out = self.fc3(out)
            return out

        def forward(self, x):
            out = self.conv_layers(x)
            out = out.view(-1, 256 * 8 * 8)
            out = self.dense_layers(out)
            return out

    return Net()