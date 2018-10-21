import numpy as np


def load_template(version):
    loc = []
    with open('zigzag_' + version + '.txt') as f:
        for line in f:
            loc.append([int(x) for x in line.rstrip('\n').split(',')])

    qr_code = []
    with open(version + '.txt') as f:
        for line in f:
            qr_code.append([1 if c == '1' else 0 for c in line.rstrip('\n')])
    qr_code = np.array(qr_code)
    return loc, qr_code


def logistic_map(width):
    size = (width * width) // 8 + 1
    x = []
    r = 4.0
    for n in range(size):
        if n == 0:
            x.append(0.1)
        else:
            x.append(r * x[-1] * (1 - x[-1]))
    x = [int(e * 255) for e in x]

    log_map = []
    for e in x:
        reverse_pixel = int_to_byte(e)[::-1]
        for b in reverse_pixel:
            log_map.append(int(b))

    log_map = log_map[:(width * width)]
    log_map = np.reshape(log_map, (width, width))

    return log_map


def int_to_byte(i):
    return bin(i)[2:].zfill(8)


def char_to_byte(c):
    s = bin(ord(c))[2:].zfill(8)
    return s


def bit_xor(s):
    res = 0
    for b in s:
        res ^= int(b)
    return res


def string_to_bytes(s):
    byte_list = []
    byte_list.append(int_to_byte(len(s)))
    for c in s:
        byte_list.append(char_to_byte(c))
        byte_list.append(int_to_byte(bit_xor(char_to_byte(c))))
    return byte_list


def bytes_to_QR_code(byte_list):
    if int(byte_list[0], 2) < 13:
        version = "v1"
    else:
        version = "v2"

    loc, qr_code = load_template(version)

    idx = 0
    for byte in byte_list:
        for b in byte:
            qr_code[loc[idx][0]][loc[idx][1]] = int(b)
            idx += 1

    padding_str = "1110110000010001"
    padding_idx = 0
    while idx < len(loc):
        qr_code[loc[idx][0]][loc[idx][1]] = int(padding_str[padding_idx])
        padding_idx = (padding_idx + 1) % len(padding_str)
        idx += 1

    return qr_code


def QR_with_logistic_map(qr_code):
    width = qr_code.shape[0]
    log_map = logistic_map(width)
    for x in range(width):
        for y in range(width):
            qr_code[x][y] ^= log_map[x][y]
    return qr_code


def QR_to_hex(qr_code):
    qr_code = qr_code.ravel()
    idx = 0
    hex_list = []
    while idx < qr_code.size:
        bit_string = ''.join(
            [str(x) for x in qr_code[idx:min((idx + 32), qr_code.size)]])
        hex_list.append(hex(int(bit_string, 2)))
        #         print(bit_string)
        idx += 32

    return ''.join(hex_list)


# main function
def QR_encode(str):
    qr_code = bytes_to_QR_code(string_to_bytes(str))
    hex_string = QR_to_hex(qr_code)
    return hex_string
