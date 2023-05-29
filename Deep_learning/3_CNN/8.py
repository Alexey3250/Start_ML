def count_parameters_conv(in_channels: int, out_channels: int, kernel_size: int, bias: bool):
    parameters = in_channels * out_channels * kernel_size * kernel_size
    if bias:
        parameters += out_channels
    return parameters
