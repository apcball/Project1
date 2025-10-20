import re

def get_cost_method_test(value):
    import re
    if value is None:
        return 'standard'
    value_str = str(value).strip().lower()
    clean = re.sub(r"[()\[\],]", ' ', value_str)
    clean = re.sub(r"\s+", ' ', clean).strip()
    fifo_patterns = [r'\bfifo\b', r'first\s+in', r'first\s+in\s+first\s+out', r'first in first out', r'first in']
    avg_patterns = [r'average', r'moving', r'weighted', r'เฉลี่ย']
    for p in fifo_patterns:
        if re.search(p, clean):
            return 'fifo'
    for p in avg_patterns:
        if re.search(p, clean):
            return 'average'
    return 'standard'

samples = [
    'CostingMethod',
    'First in First Out (FIFO)',
    'First in First Out (FIFO) ยังลงผิด',
    'Average Cost',
    'Moving Average',
    'เฉลี่ย',
    '',
    None
]
for s in samples:
    print(repr(s), '->', get_cost_method_test(s))
