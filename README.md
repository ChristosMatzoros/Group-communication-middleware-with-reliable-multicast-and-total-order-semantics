# Group-communication-middleware-with-reliable-multicast-and-total-order-semantics
We created from scratch a middleware that supports group communication with reliable multicast and total order messaging delivery. This protocol was tested with a chat application. (in Python)


### 🚀 Run Example

# 1. Activate your environment
```bash
source venv/bin/activate
```
# 2. Run manager
```bash
python3 -m manager.server 50001
```
# 3. In a new terminal:
```bash
source venv/bin/activate
python3 -m client.client Chris
```
# 4. In another terminal:
```bash
source venv/bin/activate
python3 -m client.client Akis
```

Created by:
<br />
Akis Giannoukos  www.linkedin.com/in/akisgiannoukos
<br />
Christos Matzoros   www.linkedin.com/in/matzoros-christos
