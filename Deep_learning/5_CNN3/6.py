import torch.nn as nn
from torchvision.models import resnet18, alexnet, vgg11, googlenet

def get_pretrained_model(model_name: str, num_classes: int, pretrained: bool = True):
    if model_name == "resnet18":
        model = resnet18(pretrained=pretrained)
        model.fc = nn.Linear(model.fc.in_features, num_classes)

    elif model_name == "alexnet":
        model = alexnet(pretrained=pretrained)
        model.classifier[6] = nn.Linear(model.classifier[6].in_features, num_classes)

    elif model_name == "vgg11":
        model = vgg11(pretrained=pretrained)
        model.classifier[6] = nn.Linear(model.classifier[6].in_features, num_classes)

    elif model_name == "googlenet":
        model = googlenet(pretrained=pretrained)
        model.fc = nn.Linear(model.fc.in_features, num_classes)
        if model.aux_logits:
            model.aux1.fc2 = nn.Linear(model.aux1.fc2.in_features, num_classes)
            model.aux2.fc2 = nn.Linear(model.aux2.fc2.in_features, num_classes)

    else:
        raise ValueError("Invalid model name")

    return model
