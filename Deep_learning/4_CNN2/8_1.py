from torch import nn



def create_simple_conv_cifar() -> nn.Sequential:
    model = nn.Sequential(
        nn.Conv2d(in_channels=3, out_channels=16, kernel_size=3, padding=1),  # 32 x 32 x 16
        nn.ReLU(),
        nn.MaxPool2d(2),  # 16 x 16 x 16
        nn.Conv2d(in_channels=16, out_channels=32, kernel_size=3, padding=1),  # 16 x 16 x 32
        nn.ReLU(),
        nn.MaxPool2d(2),  # 8 x 8 x 32
        nn.Flatten(),
        nn.Linear(8 * 8 * 32, 1024),
        nn.ReLU(),
        nn.Linear(1024, 128),
        nn.ReLU(),
        nn.Linear(128, 10)
    )
    return model