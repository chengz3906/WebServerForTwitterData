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


def int_to_byte(i):
    return bin(i)[2:].zfill(8)


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


def QR_with_logistic_map(qr_code):
    width = qr_code.shape[0]
    log_map = logistic_map(width)
    for x in range(width):
        for y in range(width):
            qr_code[x][y] ^= log_map[x][y]
    return qr_code


def hex_to_QR(hex_string, width):
    hex_list = list(filter(None, hex_string.split('0x')))
    bin_list = []
    for hex_string in hex_list:
        bin_list.append(bin(int(hex_string, 16))[2:])

    size = width * width
    qr_code = []
    qr_code_idx = 0
    for bin_string in bin_list:
        bit_size = min(size - qr_code_idx, 32)
        bin_string = bin_string.zfill(bit_size)
        for b in bin_string:
            qr_code.append(int(b))
            qr_code_idx += 1

    qr_code = np.reshape(qr_code, (width, width))
    return qr_code


def QR_to_bytes(qr_code):
    if qr_code.shape[0] == 21:
        loc_list, qr_code_template = load_template("v1")
    else:
        loc_list, qr_code_template = load_template("v2")

    byte_list = []
    for loc in loc_list:
        x = loc[0]
        y = loc[1]
        byte_list.append(str(qr_code[x][y]))

    size = len(byte_list) // 8 * 8
    byte_list = byte_list[:size]
    byte_list = np.reshape(byte_list, (-1, 8))
    return [''.join(x) for x in byte_list]


def bytes_to_string(byte_list):
    size = int(byte_list[0], 2)
    idx = 1
    char_list = []
    while len(char_list) < size:
        char_list.append(chr(int(byte_list[idx], 2)))
        # check error
        idx += 2

    return ''.join(char_list)


def pattern_mask(loc_list, width):
    size = width * width
    mask = [1 for x in range(size)]
    mask = np.reshape(mask, (width, width))
    for loc in loc_list:
        x = loc[0]
        y = loc[1]
        mask[x][y] = 0

    return mask


def sub_QR(qr_code, x_start, y_start, width):
    sub_qr_code = np.empty((width, width), dtype='int')
    for x in range(width):
        for y in range(width):
            sub_qr_code[x][y] = qr_code[x_start + x][y_start + y]

    return sub_qr_code


def clockwise_rotate_QR(qr_code):
    rotated_qr_code = np.empty(qr_code.shape, dtype='int')
    for x in range(qr_code.shape[0]):
        for y in range(qr_code.shape[1]):
            rotated_qr_code[y][qr_code.shape[0] - 1 - x] = qr_code[x][y]

    return rotated_qr_code


def QR_match(qr_1, qr_2, mask):
    for x in range(mask.shape[0]):
        for y in range(mask.shape[1]):
            if mask[x][y] == 1:
                if qr_1[x][y] != qr_2[x][y]:
                    return False
    return True


def extract_valid_QR_with_version(noisy_qr_code, version):
    loc_list, qr_code_template = load_template(version)
    qr_code_template = qr_code_template

    valid_width = qr_code_template.shape[0]
    mask = pattern_mask(loc_list, valid_width)
    noisy_width = noisy_qr_code.shape[0]
    width_diff = noisy_width - valid_width

    for x_start in range(width_diff + 1):
        for y_start in range(width_diff + 1):
            sub_qr = sub_QR(noisy_qr_code, x_start, y_start, valid_width)
            for _ in range(4):
                sub_qr = clockwise_rotate_QR(sub_qr)
                if QR_match(sub_qr, qr_code_template, mask):
                    return True, sub_qr

    return False, None


def extract_valid_QR(noisy_qr_code):
    noisy_qr_code = QR_with_logistic_map(noisy_qr_code)
    valid, sub_qr = extract_valid_QR_with_version(noisy_qr_code, 'v1')
    if valid:
        return valid, sub_qr
    return extract_valid_QR_with_version(noisy_qr_code, 'v2')


def QR_decode(str):
    valid, qr_code = extract_valid_QR(hex_to_QR(str, 32))
    if valid:
        return bytes_to_string(QR_to_bytes(qr_code))
    else:
        return "ERROR"