import torch
import torch.nn as nn

class Similarity1(nn.Module):
    def __init__(self):
        super().__init__()

    def forward(self, encoder_states: torch.Tensor, decoder_state: torch.Tensor):
        # encoder_states.shape = [T, N]
        # decoder_state.shape = [N]
        similarity = torch.matmul(encoder_states, decoder_state.unsqueeze(1)).squeeze(1)
        return similarity
