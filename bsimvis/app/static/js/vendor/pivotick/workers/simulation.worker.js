var to = Object.defineProperty;
var eo = (e, t, n) => t in e ? to(e, t, { enumerable: !0, configurable: !0, writable: !0, value: n }) : e[t] = n;
var E = (e, t, n) => eo(e, typeof t != "symbol" ? t + "" : t, n);
function no(e) {
  const t = +this._x.call(null, e), n = +this._y.call(null, e);
  return ar(this.cover(t, n), t, n, e);
}
function ar(e, t, n, i) {
  if (isNaN(t) || isNaN(n)) return e;
  var r, o = e._root, a = { data: i }, s = e._x0, c = e._y0, l = e._x1, f = e._y1, v, d, m, S, b, g, w, _;
  if (!o) return e._root = a, e;
  for (; o.length; )
    if ((b = t >= (v = (s + l) / 2)) ? s = v : l = v, (g = n >= (d = (c + f) / 2)) ? c = d : f = d, r = o, !(o = o[w = g << 1 | b])) return r[w] = a, e;
  if (m = +e._x.call(null, o.data), S = +e._y.call(null, o.data), t === m && n === S) return a.next = o, r ? r[w] = a : e._root = a, e;
  do
    r = r ? r[w] = new Array(4) : e._root = new Array(4), (b = t >= (v = (s + l) / 2)) ? s = v : l = v, (g = n >= (d = (c + f) / 2)) ? c = d : f = d;
  while ((w = g << 1 | b) === (_ = (S >= d) << 1 | m >= v));
  return r[_] = o, r[w] = a, e;
}
function io(e) {
  var t, n, i = e.length, r, o, a = new Array(i), s = new Array(i), c = 1 / 0, l = 1 / 0, f = -1 / 0, v = -1 / 0;
  for (n = 0; n < i; ++n)
    isNaN(r = +this._x.call(null, t = e[n])) || isNaN(o = +this._y.call(null, t)) || (a[n] = r, s[n] = o, r < c && (c = r), r > f && (f = r), o < l && (l = o), o > v && (v = o));
  if (c > f || l > v) return this;
  for (this.cover(c, l).cover(f, v), n = 0; n < i; ++n)
    ar(this, a[n], s[n], e[n]);
  return this;
}
function ro(e, t) {
  if (isNaN(e = +e) || isNaN(t = +t)) return this;
  var n = this._x0, i = this._y0, r = this._x1, o = this._y1;
  if (isNaN(n))
    r = (n = Math.floor(e)) + 1, o = (i = Math.floor(t)) + 1;
  else {
    for (var a = r - n || 1, s = this._root, c, l; n > e || e >= r || i > t || t >= o; )
      switch (l = (t < i) << 1 | e < n, c = new Array(4), c[l] = s, s = c, a *= 2, l) {
        case 0:
          r = n + a, o = i + a;
          break;
        case 1:
          n = r - a, o = i + a;
          break;
        case 2:
          r = n + a, i = o - a;
          break;
        case 3:
          n = r - a, i = o - a;
          break;
      }
    this._root && this._root.length && (this._root = s);
  }
  return this._x0 = n, this._y0 = i, this._x1 = r, this._y1 = o, this;
}
function oo() {
  var e = [];
  return this.visit(function(t) {
    if (!t.length) do
      e.push(t.data);
    while (t = t.next);
  }), e;
}
function so(e) {
  return arguments.length ? this.cover(+e[0][0], +e[0][1]).cover(+e[1][0], +e[1][1]) : isNaN(this._x0) ? void 0 : [[this._x0, this._y0], [this._x1, this._y1]];
}
function vt(e, t, n, i, r) {
  this.node = e, this.x0 = t, this.y0 = n, this.x1 = i, this.y1 = r;
}
function ao(e, t, n) {
  var i, r = this._x0, o = this._y0, a, s, c, l, f = this._x1, v = this._y1, d = [], m = this._root, S, b;
  for (m && d.push(new vt(m, r, o, f, v)), n == null ? n = 1 / 0 : (r = e - n, o = t - n, f = e + n, v = t + n, n *= n); S = d.pop(); )
    if (!(!(m = S.node) || (a = S.x0) > f || (s = S.y0) > v || (c = S.x1) < r || (l = S.y1) < o))
      if (m.length) {
        var g = (a + c) / 2, w = (s + l) / 2;
        d.push(
          new vt(m[3], g, w, c, l),
          new vt(m[2], a, w, g, l),
          new vt(m[1], g, s, c, w),
          new vt(m[0], a, s, g, w)
        ), (b = (t >= w) << 1 | e >= g) && (S = d[d.length - 1], d[d.length - 1] = d[d.length - 1 - b], d[d.length - 1 - b] = S);
      } else {
        var _ = e - +this._x.call(null, m.data), R = t - +this._y.call(null, m.data), y = _ * _ + R * R;
        if (y < n) {
          var C = Math.sqrt(n = y);
          r = e - C, o = t - C, f = e + C, v = t + C, i = m.data;
        }
      }
  return i;
}
function lo(e) {
  if (isNaN(f = +this._x.call(null, e)) || isNaN(v = +this._y.call(null, e))) return this;
  var t, n = this._root, i, r, o, a = this._x0, s = this._y0, c = this._x1, l = this._y1, f, v, d, m, S, b, g, w;
  if (!n) return this;
  if (n.length) for (; ; ) {
    if ((S = f >= (d = (a + c) / 2)) ? a = d : c = d, (b = v >= (m = (s + l) / 2)) ? s = m : l = m, t = n, !(n = n[g = b << 1 | S])) return this;
    if (!n.length) break;
    (t[g + 1 & 3] || t[g + 2 & 3] || t[g + 3 & 3]) && (i = t, w = g);
  }
  for (; n.data !== e; ) if (r = n, !(n = n.next)) return this;
  return (o = n.next) && delete n.next, r ? (o ? r.next = o : delete r.next, this) : t ? (o ? t[g] = o : delete t[g], (n = t[0] || t[1] || t[2] || t[3]) && n === (t[3] || t[2] || t[1] || t[0]) && !n.length && (i ? i[w] = n : this._root = n), this) : (this._root = o, this);
}
function co(e) {
  for (var t = 0, n = e.length; t < n; ++t) this.remove(e[t]);
  return this;
}
function uo() {
  return this._root;
}
function ho() {
  var e = 0;
  return this.visit(function(t) {
    if (!t.length) do
      ++e;
    while (t = t.next);
  }), e;
}
function fo(e) {
  var t = [], n, i = this._root, r, o, a, s, c;
  for (i && t.push(new vt(i, this._x0, this._y0, this._x1, this._y1)); n = t.pop(); )
    if (!e(i = n.node, o = n.x0, a = n.y0, s = n.x1, c = n.y1) && i.length) {
      var l = (o + s) / 2, f = (a + c) / 2;
      (r = i[3]) && t.push(new vt(r, l, f, s, c)), (r = i[2]) && t.push(new vt(r, o, f, l, c)), (r = i[1]) && t.push(new vt(r, l, a, s, f)), (r = i[0]) && t.push(new vt(r, o, a, l, f));
    }
  return this;
}
function po(e) {
  var t = [], n = [], i;
  for (this._root && t.push(new vt(this._root, this._x0, this._y0, this._x1, this._y1)); i = t.pop(); ) {
    var r = i.node;
    if (r.length) {
      var o, a = i.x0, s = i.y0, c = i.x1, l = i.y1, f = (a + c) / 2, v = (s + l) / 2;
      (o = r[0]) && t.push(new vt(o, a, s, f, v)), (o = r[1]) && t.push(new vt(o, f, s, c, v)), (o = r[2]) && t.push(new vt(o, a, v, f, l)), (o = r[3]) && t.push(new vt(o, f, v, c, l));
    }
    n.push(i);
  }
  for (; i = n.pop(); )
    e(i.node, i.x0, i.y0, i.x1, i.y1);
  return this;
}
function go(e) {
  return e[0];
}
function mo(e) {
  return arguments.length ? (this._x = e, this) : this._x;
}
function yo(e) {
  return e[1];
}
function _o(e) {
  return arguments.length ? (this._y = e, this) : this._y;
}
function hi(e, t, n) {
  var i = new fi(t ?? go, n ?? yo, NaN, NaN, NaN, NaN);
  return e == null ? i : i.addAll(e);
}
function fi(e, t, n, i, r, o) {
  this._x = e, this._y = t, this._x0 = n, this._y0 = i, this._x1 = r, this._y1 = o, this._root = void 0;
}
function Si(e) {
  for (var t = { data: e.data }, n = t; e = e.next; ) n = n.next = { data: e.data };
  return t;
}
var Tt = hi.prototype = fi.prototype;
Tt.copy = function() {
  var e = new fi(this._x, this._y, this._x0, this._y0, this._x1, this._y1), t = this._root, n, i;
  if (!t) return e;
  if (!t.length) return e._root = Si(t), e;
  for (n = [{ source: t, target: e._root = new Array(4) }]; t = n.pop(); )
    for (var r = 0; r < 4; ++r)
      (i = t.source[r]) && (i.length ? n.push({ source: i, target: t.target[r] = new Array(4) }) : t.target[r] = Si(i));
  return e;
};
Tt.add = no;
Tt.addAll = io;
Tt.cover = ro;
Tt.data = oo;
Tt.extent = so;
Tt.find = ao;
Tt.remove = lo;
Tt.removeAll = co;
Tt.root = uo;
Tt.size = ho;
Tt.visit = fo;
Tt.visitAfter = po;
Tt.x = mo;
Tt.y = _o;
function dt(e) {
  return function() {
    return e;
  };
}
function re(e) {
  return (e() - 0.5) * 1e-6;
}
function vo(e) {
  return e.x + e.vx;
}
function bo(e) {
  return e.y + e.vy;
}
function To(e) {
  var t, n, i, r = 1, o = 1;
  typeof e != "function" && (e = dt(e == null ? 1 : +e));
  function a() {
    for (var l, f = t.length, v, d, m, S, b, g, w = 0; w < o; ++w)
      for (v = hi(t, vo, bo).visitAfter(s), l = 0; l < f; ++l)
        d = t[l], b = n[d.index], g = b * b, m = d.x + d.vx, S = d.y + d.vy, v.visit(_);
    function _(R, y, C, O, B) {
      var P = R.data, L = R.r, z = b + L;
      if (P) {
        if (P.index > d.index) {
          var Y = m - P.x - P.vx, nt = S - P.y - P.vy, N = Y * Y + nt * nt;
          N < z * z && (Y === 0 && (Y = re(i), N += Y * Y), nt === 0 && (nt = re(i), N += nt * nt), N = (z - (N = Math.sqrt(N))) / N * r, d.vx += (Y *= N) * (z = (L *= L) / (g + L)), d.vy += (nt *= N) * z, P.vx -= Y * (z = 1 - z), P.vy -= nt * z);
        }
        return;
      }
      return y > m + z || O < m - z || C > S + z || B < S - z;
    }
  }
  function s(l) {
    if (l.data) return l.r = n[l.data.index];
    for (var f = l.r = 0; f < 4; ++f)
      l[f] && l[f].r > l.r && (l.r = l[f].r);
  }
  function c() {
    if (t) {
      var l, f = t.length, v;
      for (n = new Array(f), l = 0; l < f; ++l) v = t[l], n[v.index] = +e(v, l, t);
    }
  }
  return a.initialize = function(l, f) {
    t = l, i = f, c();
  }, a.iterations = function(l) {
    return arguments.length ? (o = +l, a) : o;
  }, a.strength = function(l) {
    return arguments.length ? (r = +l, a) : r;
  }, a.radius = function(l) {
    return arguments.length ? (e = typeof l == "function" ? l : dt(+l), c(), a) : e;
  }, a;
}
function wo(e) {
  return e.index;
}
function Ai(e, t) {
  var n = e.get(t);
  if (!n) throw new Error("node not found: " + t);
  return n;
}
function So(e) {
  var t = wo, n = v, i, r = dt(30), o, a, s, c, l, f = 1;
  e == null && (e = []);
  function v(g) {
    return 1 / Math.min(s[g.source.index], s[g.target.index]);
  }
  function d(g) {
    for (var w = 0, _ = e.length; w < f; ++w)
      for (var R = 0, y, C, O, B, P, L, z; R < _; ++R)
        y = e[R], C = y.source, O = y.target, B = O.x + O.vx - C.x - C.vx || re(l), P = O.y + O.vy - C.y - C.vy || re(l), L = Math.sqrt(B * B + P * P), L = (L - o[R]) / L * g * i[R], B *= L, P *= L, O.vx -= B * (z = c[R]), O.vy -= P * z, C.vx += B * (z = 1 - z), C.vy += P * z;
  }
  function m() {
    if (a) {
      var g, w = a.length, _ = e.length, R = new Map(a.map((C, O) => [t(C, O, a), C])), y;
      for (g = 0, s = new Array(w); g < _; ++g)
        y = e[g], y.index = g, typeof y.source != "object" && (y.source = Ai(R, y.source)), typeof y.target != "object" && (y.target = Ai(R, y.target)), s[y.source.index] = (s[y.source.index] || 0) + 1, s[y.target.index] = (s[y.target.index] || 0) + 1;
      for (g = 0, c = new Array(_); g < _; ++g)
        y = e[g], c[g] = s[y.source.index] / (s[y.source.index] + s[y.target.index]);
      i = new Array(_), S(), o = new Array(_), b();
    }
  }
  function S() {
    if (a)
      for (var g = 0, w = e.length; g < w; ++g)
        i[g] = +n(e[g], g, e);
  }
  function b() {
    if (a)
      for (var g = 0, w = e.length; g < w; ++g)
        o[g] = +r(e[g], g, e);
  }
  return d.initialize = function(g, w) {
    a = g, l = w, m();
  }, d.links = function(g) {
    return arguments.length ? (e = g, m(), d) : e;
  }, d.id = function(g) {
    return arguments.length ? (t = g, d) : t;
  }, d.iterations = function(g) {
    return arguments.length ? (f = +g, d) : f;
  }, d.strength = function(g) {
    return arguments.length ? (n = typeof g == "function" ? g : dt(+g), S(), d) : n;
  }, d.distance = function(g) {
    return arguments.length ? (r = typeof g == "function" ? g : dt(+g), b(), d) : r;
  }, d;
}
var Ao = { value: () => {
} };
function di() {
  for (var e = 0, t = arguments.length, n = {}, i; e < t; ++e) {
    if (!(i = arguments[e] + "") || i in n || /[\s.]/.test(i)) throw new Error("illegal type: " + i);
    n[i] = [];
  }
  return new mn(n);
}
function mn(e) {
  this._ = e;
}
function Eo(e, t) {
  return e.trim().split(/^|\s+/).map(function(n) {
    var i = "", r = n.indexOf(".");
    if (r >= 0 && (i = n.slice(r + 1), n = n.slice(0, r)), n && !t.hasOwnProperty(n)) throw new Error("unknown type: " + n);
    return { type: n, name: i };
  });
}
mn.prototype = di.prototype = {
  constructor: mn,
  on: function(e, t) {
    var n = this._, i = Eo(e + "", n), r, o = -1, a = i.length;
    if (arguments.length < 2) {
      for (; ++o < a; ) if ((r = (e = i[o]).type) && (r = xo(n[r], e.name))) return r;
      return;
    }
    if (t != null && typeof t != "function") throw new Error("invalid callback: " + t);
    for (; ++o < a; )
      if (r = (e = i[o]).type) n[r] = Ei(n[r], e.name, t);
      else if (t == null) for (r in n) n[r] = Ei(n[r], e.name, null);
    return this;
  },
  copy: function() {
    var e = {}, t = this._;
    for (var n in t) e[n] = t[n].slice();
    return new mn(e);
  },
  call: function(e, t) {
    if ((r = arguments.length - 2) > 0) for (var n = new Array(r), i = 0, r, o; i < r; ++i) n[i] = arguments[i + 2];
    if (!this._.hasOwnProperty(e)) throw new Error("unknown type: " + e);
    for (o = this._[e], i = 0, r = o.length; i < r; ++i) o[i].value.apply(t, n);
  },
  apply: function(e, t, n) {
    if (!this._.hasOwnProperty(e)) throw new Error("unknown type: " + e);
    for (var i = this._[e], r = 0, o = i.length; r < o; ++r) i[r].value.apply(t, n);
  }
};
function xo(e, t) {
  for (var n = 0, i = e.length, r; n < i; ++n)
    if ((r = e[n]).name === t)
      return r.value;
}
function Ei(e, t, n) {
  for (var i = 0, r = e.length; i < r; ++i)
    if (e[i].name === t) {
      e[i] = Ao, e = e.slice(0, i).concat(e.slice(i + 1));
      break;
    }
  return n != null && e.push({ name: t, value: n }), e;
}
var we = 0, je = 0, ze = 0, lr = 1e3, vn, He, bn = 0, he = 0, Mn = 0, Ve = typeof performance == "object" && performance.now ? performance : Date, cr = typeof window == "object" && window.requestAnimationFrame ? window.requestAnimationFrame.bind(window) : function(e) {
  setTimeout(e, 17);
};
function ur() {
  return he || (cr(No), he = Ve.now() + Mn);
}
function No() {
  he = 0;
}
function ri() {
  this._call = this._time = this._next = null;
}
ri.prototype = hr.prototype = {
  constructor: ri,
  restart: function(e, t, n) {
    if (typeof e != "function") throw new TypeError("callback is not a function");
    n = (n == null ? ur() : +n) + (t == null ? 0 : +t), !this._next && He !== this && (He ? He._next = this : vn = this, He = this), this._call = e, this._time = n, oi();
  },
  stop: function() {
    this._call && (this._call = null, this._time = 1 / 0, oi());
  }
};
function hr(e, t, n) {
  var i = new ri();
  return i.restart(e, t, n), i;
}
function Ro() {
  ur(), ++we;
  for (var e = vn, t; e; )
    (t = he - e._time) >= 0 && e._call.call(void 0, t), e = e._next;
  --we;
}
function xi() {
  he = (bn = Ve.now()) + Mn, we = je = 0;
  try {
    Ro();
  } finally {
    we = 0, Io(), he = 0;
  }
}
function Mo() {
  var e = Ve.now(), t = e - bn;
  t > lr && (Mn -= t, bn = e);
}
function Io() {
  for (var e, t = vn, n, i = 1 / 0; t; )
    t._call ? (i > t._time && (i = t._time), e = t, t = t._next) : (n = t._next, t._next = null, t = e ? e._next = n : vn = n);
  He = e, oi(i);
}
function oi(e) {
  if (!we) {
    je && (je = clearTimeout(je));
    var t = e - he;
    t > 24 ? (e < 1 / 0 && (je = setTimeout(xi, e - Ve.now() - Mn)), ze && (ze = clearInterval(ze))) : (ze || (bn = Ve.now(), ze = setInterval(Mo, lr)), we = 1, cr(xi));
  }
}
const Co = 1664525, Do = 1013904223, Ni = 4294967296;
function Oo() {
  let e = 1;
  return () => (e = (Co * e + Do) % Ni) / Ni;
}
function ko(e) {
  return e.x;
}
function Lo(e) {
  return e.y;
}
var Fo = 10, Po = Math.PI * (3 - Math.sqrt(5));
function zo(e) {
  var t, n = 1, i = 1e-3, r = 1 - Math.pow(i, 1 / 300), o = 0, a = 0.6, s = /* @__PURE__ */ new Map(), c = hr(v), l = di("tick", "end"), f = Oo();
  e == null && (e = []);
  function v() {
    d(), l.call("tick", t), n < i && (c.stop(), l.call("end", t));
  }
  function d(b) {
    var g, w = e.length, _;
    b === void 0 && (b = 1);
    for (var R = 0; R < b; ++R)
      for (n += (o - n) * r, s.forEach(function(y) {
        y(n);
      }), g = 0; g < w; ++g)
        _ = e[g], _.fx == null ? _.x += _.vx *= a : (_.x = _.fx, _.vx = 0), _.fy == null ? _.y += _.vy *= a : (_.y = _.fy, _.vy = 0);
    return t;
  }
  function m() {
    for (var b = 0, g = e.length, w; b < g; ++b) {
      if (w = e[b], w.index = b, w.fx != null && (w.x = w.fx), w.fy != null && (w.y = w.fy), isNaN(w.x) || isNaN(w.y)) {
        var _ = Fo * Math.sqrt(0.5 + b), R = b * Po;
        w.x = _ * Math.cos(R), w.y = _ * Math.sin(R);
      }
      (isNaN(w.vx) || isNaN(w.vy)) && (w.vx = w.vy = 0);
    }
  }
  function S(b) {
    return b.initialize && b.initialize(e, f), b;
  }
  return m(), t = {
    tick: d,
    restart: function() {
      return c.restart(v), t;
    },
    stop: function() {
      return c.stop(), t;
    },
    nodes: function(b) {
      return arguments.length ? (e = b, m(), s.forEach(S), t) : e;
    },
    alpha: function(b) {
      return arguments.length ? (n = +b, t) : n;
    },
    alphaMin: function(b) {
      return arguments.length ? (i = +b, t) : i;
    },
    alphaDecay: function(b) {
      return arguments.length ? (r = +b, t) : +r;
    },
    alphaTarget: function(b) {
      return arguments.length ? (o = +b, t) : o;
    },
    velocityDecay: function(b) {
      return arguments.length ? (a = 1 - b, t) : 1 - a;
    },
    randomSource: function(b) {
      return arguments.length ? (f = b, s.forEach(S), t) : f;
    },
    force: function(b, g) {
      return arguments.length > 1 ? (g == null ? s.delete(b) : s.set(b, S(g)), t) : s.get(b);
    },
    find: function(b, g, w) {
      var _ = 0, R = e.length, y, C, O, B, P;
      for (w == null ? w = 1 / 0 : w *= w, _ = 0; _ < R; ++_)
        B = e[_], y = b - B.x, C = g - B.y, O = y * y + C * C, O < w && (P = B, w = O);
      return P;
    },
    on: function(b, g) {
      return arguments.length > 1 ? (l.on(b, g), t) : l.on(b);
    }
  };
}
function Bo() {
  var e, t, n, i, r = dt(-30), o, a = 1, s = 1 / 0, c = 0.81;
  function l(m) {
    var S, b = e.length, g = hi(e, ko, Lo).visitAfter(v);
    for (i = m, S = 0; S < b; ++S) t = e[S], g.visit(d);
  }
  function f() {
    if (e) {
      var m, S = e.length, b;
      for (o = new Array(S), m = 0; m < S; ++m) b = e[m], o[b.index] = +r(b, m, e);
    }
  }
  function v(m) {
    var S = 0, b, g, w = 0, _, R, y;
    if (m.length) {
      for (_ = R = y = 0; y < 4; ++y)
        (b = m[y]) && (g = Math.abs(b.value)) && (S += b.value, w += g, _ += g * b.x, R += g * b.y);
      m.x = _ / w, m.y = R / w;
    } else {
      b = m, b.x = b.data.x, b.y = b.data.y;
      do
        S += o[b.data.index];
      while (b = b.next);
    }
    m.value = S;
  }
  function d(m, S, b, g) {
    if (!m.value) return !0;
    var w = m.x - t.x, _ = m.y - t.y, R = g - S, y = w * w + _ * _;
    if (R * R / c < y)
      return y < s && (w === 0 && (w = re(n), y += w * w), _ === 0 && (_ = re(n), y += _ * _), y < a && (y = Math.sqrt(a * y)), t.vx += w * m.value * i / y, t.vy += _ * m.value * i / y), !0;
    if (m.length || y >= s) return;
    (m.data !== t || m.next) && (w === 0 && (w = re(n), y += w * w), _ === 0 && (_ = re(n), y += _ * _), y < a && (y = Math.sqrt(a * y)));
    do
      m.data !== t && (R = o[m.data.index] * i / y, t.vx += w * R, t.vy += _ * R);
    while (m = m.next);
  }
  return l.initialize = function(m, S) {
    e = m, n = S, f();
  }, l.strength = function(m) {
    return arguments.length ? (r = typeof m == "function" ? m : dt(+m), f(), l) : r;
  }, l.distanceMin = function(m) {
    return arguments.length ? (a = m * m, l) : Math.sqrt(a);
  }, l.distanceMax = function(m) {
    return arguments.length ? (s = m * m, l) : Math.sqrt(s);
  }, l.theta = function(m) {
    return arguments.length ? (c = m * m, l) : Math.sqrt(c);
  }, l;
}
function Ri(e, t, n) {
  var i, r = dt(0.1), o, a;
  typeof e != "function" && (e = dt(+e)), t == null && (t = 0), n == null && (n = 0);
  function s(l) {
    for (var f = 0, v = i.length; f < v; ++f) {
      var d = i[f], m = d.x - t || 1e-6, S = d.y - n || 1e-6, b = Math.sqrt(m * m + S * S), g = (a[f] - b) * o[f] * l / b;
      d.vx += m * g, d.vy += S * g;
    }
  }
  function c() {
    if (i) {
      var l, f = i.length;
      for (o = new Array(f), a = new Array(f), l = 0; l < f; ++l)
        a[l] = +e(i[l], l, i), o[l] = isNaN(a[l]) ? 0 : +r(i[l], l, i);
    }
  }
  return s.initialize = function(l) {
    i = l, c();
  }, s.strength = function(l) {
    return arguments.length ? (r = typeof l == "function" ? l : dt(+l), c(), s) : r;
  }, s.radius = function(l) {
    return arguments.length ? (e = typeof l == "function" ? l : dt(+l), c(), s) : e;
  }, s.x = function(l) {
    return arguments.length ? (t = +l, s) : t;
  }, s.y = function(l) {
    return arguments.length ? (n = +l, s) : n;
  }, s;
}
function Mi(e) {
  var t = dt(0.1), n, i, r;
  typeof e != "function" && (e = dt(e == null ? 0 : +e));
  function o(s) {
    for (var c = 0, l = n.length, f; c < l; ++c)
      f = n[c], f.vx += (r[c] - f.x) * i[c] * s;
  }
  function a() {
    if (n) {
      var s, c = n.length;
      for (i = new Array(c), r = new Array(c), s = 0; s < c; ++s)
        i[s] = isNaN(r[s] = +e(n[s], s, n)) ? 0 : +t(n[s], s, n);
    }
  }
  return o.initialize = function(s) {
    n = s, a();
  }, o.strength = function(s) {
    return arguments.length ? (t = typeof s == "function" ? s : dt(+s), a(), o) : t;
  }, o.x = function(s) {
    return arguments.length ? (e = typeof s == "function" ? s : dt(+s), a(), o) : e;
  }, o;
}
function Ii(e) {
  var t = dt(0.1), n, i, r;
  typeof e != "function" && (e = dt(e == null ? 0 : +e));
  function o(s) {
    for (var c = 0, l = n.length, f; c < l; ++c)
      f = n[c], f.vy += (r[c] - f.y) * i[c] * s;
  }
  function a() {
    if (n) {
      var s, c = n.length;
      for (i = new Array(c), r = new Array(c), s = 0; s < c; ++s)
        i[s] = isNaN(r[s] = +e(n[s], s, n)) ? 0 : +t(n[s], s, n);
    }
  }
  return o.initialize = function(s) {
    n = s, a();
  }, o.strength = function(s) {
    return arguments.length ? (t = typeof s == "function" ? s : dt(+s), a(), o) : t;
  }, o.y = function(s) {
    return arguments.length ? (e = typeof s == "function" ? s : dt(+s), a(), o) : e;
  }, o;
}
function Go(e = 0, t = 0, n = 1e-3) {
  let i = [], r;
  function o() {
    r = typeof n == "function" ? n : () => n;
  }
  function a(s) {
    for (let c = 0, l = i.length; c < l; ++c) {
      const f = i[c], v = r(f, c, i);
      f.vx != null && f.x != null && (f.vx -= (f.x - e) * v * s), f.vy != null && f.y != null && (f.vy -= (f.y - t) * v * s);
    }
  }
  return a.initialize = (s) => {
    i = s, o();
  }, a.x = function(s) {
    return arguments.length ? (e = s, a) : e;
  }, a.y = function(s) {
    return arguments.length ? (t = s, a) : t;
  }, a.strength = function(s) {
    return arguments.length ? (n = s, o(), a) : n;
  }, a;
}
var si = "http://www.w3.org/1999/xhtml";
const Ci = {
  svg: "http://www.w3.org/2000/svg",
  xhtml: si,
  xlink: "http://www.w3.org/1999/xlink",
  xml: "http://www.w3.org/XML/1998/namespace",
  xmlns: "http://www.w3.org/2000/xmlns/"
};
function fr(e) {
  var t = e += "", n = t.indexOf(":");
  return n >= 0 && (t = e.slice(0, n)) !== "xmlns" && (e = e.slice(n + 1)), Ci.hasOwnProperty(t) ? { space: Ci[t], local: e } : e;
}
function Uo(e) {
  return function() {
    var t = this.ownerDocument, n = this.namespaceURI;
    return n === si && t.documentElement.namespaceURI === si ? t.createElement(e) : t.createElementNS(n, e);
  };
}
function jo(e) {
  return function() {
    return this.ownerDocument.createElementNS(e.space, e.local);
  };
}
function dr(e) {
  var t = fr(e);
  return (t.local ? jo : Uo)(t);
}
function Ho() {
}
function pr(e) {
  return e == null ? Ho : function() {
    return this.querySelector(e);
  };
}
function Wo(e) {
  typeof e != "function" && (e = pr(e));
  for (var t = this._groups, n = t.length, i = new Array(n), r = 0; r < n; ++r)
    for (var o = t[r], a = o.length, s = i[r] = new Array(a), c, l, f = 0; f < a; ++f)
      (c = o[f]) && (l = e.call(c, c.__data__, f, o)) && ("__data__" in c && (l.__data__ = c.__data__), s[f] = l);
  return new It(i, this._parents);
}
function Ko(e) {
  return e == null ? [] : Array.isArray(e) ? e : Array.from(e);
}
function Vo() {
  return [];
}
function $o(e) {
  return e == null ? Vo : function() {
    return this.querySelectorAll(e);
  };
}
function Yo(e) {
  return function() {
    return Ko(e.apply(this, arguments));
  };
}
function qo(e) {
  typeof e == "function" ? e = Yo(e) : e = $o(e);
  for (var t = this._groups, n = t.length, i = [], r = [], o = 0; o < n; ++o)
    for (var a = t[o], s = a.length, c, l = 0; l < s; ++l)
      (c = a[l]) && (i.push(e.call(c, c.__data__, l, a)), r.push(c));
  return new It(i, r);
}
function Xo(e) {
  return function() {
    return this.matches(e);
  };
}
function gr(e) {
  return function(t) {
    return t.matches(e);
  };
}
var Zo = Array.prototype.find;
function Qo(e) {
  return function() {
    return Zo.call(this.children, e);
  };
}
function Jo() {
  return this.firstElementChild;
}
function ts(e) {
  return this.select(e == null ? Jo : Qo(typeof e == "function" ? e : gr(e)));
}
var es = Array.prototype.filter;
function ns() {
  return Array.from(this.children);
}
function is(e) {
  return function() {
    return es.call(this.children, e);
  };
}
function rs(e) {
  return this.selectAll(e == null ? ns : is(typeof e == "function" ? e : gr(e)));
}
function os(e) {
  typeof e != "function" && (e = Xo(e));
  for (var t = this._groups, n = t.length, i = new Array(n), r = 0; r < n; ++r)
    for (var o = t[r], a = o.length, s = i[r] = [], c, l = 0; l < a; ++l)
      (c = o[l]) && e.call(c, c.__data__, l, o) && s.push(c);
  return new It(i, this._parents);
}
function mr(e) {
  return new Array(e.length);
}
function ss() {
  return new It(this._enter || this._groups.map(mr), this._parents);
}
function Tn(e, t) {
  this.ownerDocument = e.ownerDocument, this.namespaceURI = e.namespaceURI, this._next = null, this._parent = e, this.__data__ = t;
}
Tn.prototype = {
  constructor: Tn,
  appendChild: function(e) {
    return this._parent.insertBefore(e, this._next);
  },
  insertBefore: function(e, t) {
    return this._parent.insertBefore(e, t);
  },
  querySelector: function(e) {
    return this._parent.querySelector(e);
  },
  querySelectorAll: function(e) {
    return this._parent.querySelectorAll(e);
  }
};
function as(e) {
  return function() {
    return e;
  };
}
function ls(e, t, n, i, r, o) {
  for (var a = 0, s, c = t.length, l = o.length; a < l; ++a)
    (s = t[a]) ? (s.__data__ = o[a], i[a] = s) : n[a] = new Tn(e, o[a]);
  for (; a < c; ++a)
    (s = t[a]) && (r[a] = s);
}
function cs(e, t, n, i, r, o, a) {
  var s, c, l = /* @__PURE__ */ new Map(), f = t.length, v = o.length, d = new Array(f), m;
  for (s = 0; s < f; ++s)
    (c = t[s]) && (d[s] = m = a.call(c, c.__data__, s, t) + "", l.has(m) ? r[s] = c : l.set(m, c));
  for (s = 0; s < v; ++s)
    m = a.call(e, o[s], s, o) + "", (c = l.get(m)) ? (i[s] = c, c.__data__ = o[s], l.delete(m)) : n[s] = new Tn(e, o[s]);
  for (s = 0; s < f; ++s)
    (c = t[s]) && l.get(d[s]) === c && (r[s] = c);
}
function us(e) {
  return e.__data__;
}
function hs(e, t) {
  if (!arguments.length) return Array.from(this, us);
  var n = t ? cs : ls, i = this._parents, r = this._groups;
  typeof e != "function" && (e = as(e));
  for (var o = r.length, a = new Array(o), s = new Array(o), c = new Array(o), l = 0; l < o; ++l) {
    var f = i[l], v = r[l], d = v.length, m = fs(e.call(f, f && f.__data__, l, i)), S = m.length, b = s[l] = new Array(S), g = a[l] = new Array(S), w = c[l] = new Array(d);
    n(f, v, b, g, w, m, t);
    for (var _ = 0, R = 0, y, C; _ < S; ++_)
      if (y = b[_]) {
        for (_ >= R && (R = _ + 1); !(C = g[R]) && ++R < S; ) ;
        y._next = C || null;
      }
  }
  return a = new It(a, i), a._enter = s, a._exit = c, a;
}
function fs(e) {
  return typeof e == "object" && "length" in e ? e : Array.from(e);
}
function ds() {
  return new It(this._exit || this._groups.map(mr), this._parents);
}
function ps(e, t, n) {
  var i = this.enter(), r = this, o = this.exit();
  return typeof e == "function" ? (i = e(i), i && (i = i.selection())) : i = i.append(e + ""), t != null && (r = t(r), r && (r = r.selection())), n == null ? o.remove() : n(o), i && r ? i.merge(r).order() : r;
}
function gs(e) {
  for (var t = e.selection ? e.selection() : e, n = this._groups, i = t._groups, r = n.length, o = i.length, a = Math.min(r, o), s = new Array(r), c = 0; c < a; ++c)
    for (var l = n[c], f = i[c], v = l.length, d = s[c] = new Array(v), m, S = 0; S < v; ++S)
      (m = l[S] || f[S]) && (d[S] = m);
  for (; c < r; ++c)
    s[c] = n[c];
  return new It(s, this._parents);
}
function ms() {
  for (var e = this._groups, t = -1, n = e.length; ++t < n; )
    for (var i = e[t], r = i.length - 1, o = i[r], a; --r >= 0; )
      (a = i[r]) && (o && a.compareDocumentPosition(o) ^ 4 && o.parentNode.insertBefore(a, o), o = a);
  return this;
}
function ys(e) {
  e || (e = _s);
  function t(v, d) {
    return v && d ? e(v.__data__, d.__data__) : !v - !d;
  }
  for (var n = this._groups, i = n.length, r = new Array(i), o = 0; o < i; ++o) {
    for (var a = n[o], s = a.length, c = r[o] = new Array(s), l, f = 0; f < s; ++f)
      (l = a[f]) && (c[f] = l);
    c.sort(t);
  }
  return new It(r, this._parents).order();
}
function _s(e, t) {
  return e < t ? -1 : e > t ? 1 : e >= t ? 0 : NaN;
}
function vs() {
  var e = arguments[0];
  return arguments[0] = this, e.apply(null, arguments), this;
}
function bs() {
  return Array.from(this);
}
function Ts() {
  for (var e = this._groups, t = 0, n = e.length; t < n; ++t)
    for (var i = e[t], r = 0, o = i.length; r < o; ++r) {
      var a = i[r];
      if (a) return a;
    }
  return null;
}
function ws() {
  let e = 0;
  for (const t of this) ++e;
  return e;
}
function Ss() {
  return !this.node();
}
function As(e) {
  for (var t = this._groups, n = 0, i = t.length; n < i; ++n)
    for (var r = t[n], o = 0, a = r.length, s; o < a; ++o)
      (s = r[o]) && e.call(s, s.__data__, o, r);
  return this;
}
function Es(e) {
  return function() {
    this.removeAttribute(e);
  };
}
function xs(e) {
  return function() {
    this.removeAttributeNS(e.space, e.local);
  };
}
function Ns(e, t) {
  return function() {
    this.setAttribute(e, t);
  };
}
function Rs(e, t) {
  return function() {
    this.setAttributeNS(e.space, e.local, t);
  };
}
function Ms(e, t) {
  return function() {
    var n = t.apply(this, arguments);
    n == null ? this.removeAttribute(e) : this.setAttribute(e, n);
  };
}
function Is(e, t) {
  return function() {
    var n = t.apply(this, arguments);
    n == null ? this.removeAttributeNS(e.space, e.local) : this.setAttributeNS(e.space, e.local, n);
  };
}
function Cs(e, t) {
  var n = fr(e);
  if (arguments.length < 2) {
    var i = this.node();
    return n.local ? i.getAttributeNS(n.space, n.local) : i.getAttribute(n);
  }
  return this.each((t == null ? n.local ? xs : Es : typeof t == "function" ? n.local ? Is : Ms : n.local ? Rs : Ns)(n, t));
}
function yr(e) {
  return e.ownerDocument && e.ownerDocument.defaultView || e.document && e || e.defaultView;
}
function Ds(e) {
  return function() {
    this.style.removeProperty(e);
  };
}
function Os(e, t, n) {
  return function() {
    this.style.setProperty(e, t, n);
  };
}
function ks(e, t, n) {
  return function() {
    var i = t.apply(this, arguments);
    i == null ? this.style.removeProperty(e) : this.style.setProperty(e, i, n);
  };
}
function Ls(e, t, n) {
  return arguments.length > 1 ? this.each((t == null ? Ds : typeof t == "function" ? ks : Os)(e, t, n ?? "")) : Fs(this.node(), e);
}
function Fs(e, t) {
  return e.style.getPropertyValue(t) || yr(e).getComputedStyle(e, null).getPropertyValue(t);
}
function Ps(e) {
  return function() {
    delete this[e];
  };
}
function zs(e, t) {
  return function() {
    this[e] = t;
  };
}
function Bs(e, t) {
  return function() {
    var n = t.apply(this, arguments);
    n == null ? delete this[e] : this[e] = n;
  };
}
function Gs(e, t) {
  return arguments.length > 1 ? this.each((t == null ? Ps : typeof t == "function" ? Bs : zs)(e, t)) : this.node()[e];
}
function _r(e) {
  return e.trim().split(/^|\s+/);
}
function pi(e) {
  return e.classList || new vr(e);
}
function vr(e) {
  this._node = e, this._names = _r(e.getAttribute("class") || "");
}
vr.prototype = {
  add: function(e) {
    var t = this._names.indexOf(e);
    t < 0 && (this._names.push(e), this._node.setAttribute("class", this._names.join(" ")));
  },
  remove: function(e) {
    var t = this._names.indexOf(e);
    t >= 0 && (this._names.splice(t, 1), this._node.setAttribute("class", this._names.join(" ")));
  },
  contains: function(e) {
    return this._names.indexOf(e) >= 0;
  }
};
function br(e, t) {
  for (var n = pi(e), i = -1, r = t.length; ++i < r; ) n.add(t[i]);
}
function Tr(e, t) {
  for (var n = pi(e), i = -1, r = t.length; ++i < r; ) n.remove(t[i]);
}
function Us(e) {
  return function() {
    br(this, e);
  };
}
function js(e) {
  return function() {
    Tr(this, e);
  };
}
function Hs(e, t) {
  return function() {
    (t.apply(this, arguments) ? br : Tr)(this, e);
  };
}
function Ws(e, t) {
  var n = _r(e + "");
  if (arguments.length < 2) {
    for (var i = pi(this.node()), r = -1, o = n.length; ++r < o; ) if (!i.contains(n[r])) return !1;
    return !0;
  }
  return this.each((typeof t == "function" ? Hs : t ? Us : js)(n, t));
}
function Ks() {
  this.textContent = "";
}
function Vs(e) {
  return function() {
    this.textContent = e;
  };
}
function $s(e) {
  return function() {
    var t = e.apply(this, arguments);
    this.textContent = t ?? "";
  };
}
function Ys(e) {
  return arguments.length ? this.each(e == null ? Ks : (typeof e == "function" ? $s : Vs)(e)) : this.node().textContent;
}
function qs() {
  this.innerHTML = "";
}
function Xs(e) {
  return function() {
    this.innerHTML = e;
  };
}
function Zs(e) {
  return function() {
    var t = e.apply(this, arguments);
    this.innerHTML = t ?? "";
  };
}
function Qs(e) {
  return arguments.length ? this.each(e == null ? qs : (typeof e == "function" ? Zs : Xs)(e)) : this.node().innerHTML;
}
function Js() {
  this.nextSibling && this.parentNode.appendChild(this);
}
function ta() {
  return this.each(Js);
}
function ea() {
  this.previousSibling && this.parentNode.insertBefore(this, this.parentNode.firstChild);
}
function na() {
  return this.each(ea);
}
function ia(e) {
  var t = typeof e == "function" ? e : dr(e);
  return this.select(function() {
    return this.appendChild(t.apply(this, arguments));
  });
}
function ra() {
  return null;
}
function oa(e, t) {
  var n = typeof e == "function" ? e : dr(e), i = t == null ? ra : typeof t == "function" ? t : pr(t);
  return this.select(function() {
    return this.insertBefore(n.apply(this, arguments), i.apply(this, arguments) || null);
  });
}
function sa() {
  var e = this.parentNode;
  e && e.removeChild(this);
}
function aa() {
  return this.each(sa);
}
function la() {
  var e = this.cloneNode(!1), t = this.parentNode;
  return t ? t.insertBefore(e, this.nextSibling) : e;
}
function ca() {
  var e = this.cloneNode(!0), t = this.parentNode;
  return t ? t.insertBefore(e, this.nextSibling) : e;
}
function ua(e) {
  return this.select(e ? ca : la);
}
function ha(e) {
  return arguments.length ? this.property("__data__", e) : this.node().__data__;
}
function fa(e) {
  return function(t) {
    e.call(this, t, this.__data__);
  };
}
function da(e) {
  return e.trim().split(/^|\s+/).map(function(t) {
    var n = "", i = t.indexOf(".");
    return i >= 0 && (n = t.slice(i + 1), t = t.slice(0, i)), { type: t, name: n };
  });
}
function pa(e) {
  return function() {
    var t = this.__on;
    if (t) {
      for (var n = 0, i = -1, r = t.length, o; n < r; ++n)
        o = t[n], (!e.type || o.type === e.type) && o.name === e.name ? this.removeEventListener(o.type, o.listener, o.options) : t[++i] = o;
      ++i ? t.length = i : delete this.__on;
    }
  };
}
function ga(e, t, n) {
  return function() {
    var i = this.__on, r, o = fa(t);
    if (i) {
      for (var a = 0, s = i.length; a < s; ++a)
        if ((r = i[a]).type === e.type && r.name === e.name) {
          this.removeEventListener(r.type, r.listener, r.options), this.addEventListener(r.type, r.listener = o, r.options = n), r.value = t;
          return;
        }
    }
    this.addEventListener(e.type, o, n), r = { type: e.type, name: e.name, value: t, listener: o, options: n }, i ? i.push(r) : this.__on = [r];
  };
}
function ma(e, t, n) {
  var i = da(e + ""), r, o = i.length, a;
  if (arguments.length < 2) {
    var s = this.node().__on;
    if (s) {
      for (var c = 0, l = s.length, f; c < l; ++c)
        for (r = 0, f = s[c]; r < o; ++r)
          if ((a = i[r]).type === f.type && a.name === f.name)
            return f.value;
    }
    return;
  }
  for (s = t ? ga : pa, r = 0; r < o; ++r) this.each(s(i[r], t, n));
  return this;
}
function wr(e, t, n) {
  var i = yr(e), r = i.CustomEvent;
  typeof r == "function" ? r = new r(t, n) : (r = i.document.createEvent("Event"), n ? (r.initEvent(t, n.bubbles, n.cancelable), r.detail = n.detail) : r.initEvent(t, !1, !1)), e.dispatchEvent(r);
}
function ya(e, t) {
  return function() {
    return wr(this, e, t);
  };
}
function _a(e, t) {
  return function() {
    return wr(this, e, t.apply(this, arguments));
  };
}
function va(e, t) {
  return this.each((typeof t == "function" ? _a : ya)(e, t));
}
function* ba() {
  for (var e = this._groups, t = 0, n = e.length; t < n; ++t)
    for (var i = e[t], r = 0, o = i.length, a; r < o; ++r)
      (a = i[r]) && (yield a);
}
var Ta = [null];
function It(e, t) {
  this._groups = e, this._parents = t;
}
function wa() {
  return this;
}
It.prototype = {
  constructor: It,
  select: Wo,
  selectAll: qo,
  selectChild: ts,
  selectChildren: rs,
  filter: os,
  data: hs,
  enter: ss,
  exit: ds,
  join: ps,
  merge: gs,
  selection: wa,
  order: ms,
  sort: ys,
  call: vs,
  nodes: bs,
  node: Ts,
  size: ws,
  empty: Ss,
  each: As,
  attr: Cs,
  style: Ls,
  property: Gs,
  classed: Ws,
  text: Ys,
  html: Qs,
  raise: ta,
  lower: na,
  append: ia,
  insert: oa,
  remove: aa,
  clone: ua,
  datum: ha,
  on: ma,
  dispatch: va,
  [Symbol.iterator]: ba
};
function wn(e) {
  return typeof e == "string" ? new It([[document.querySelector(e)]], [document.documentElement]) : new It([[e]], Ta);
}
function Sa(e) {
  let t;
  for (; t = e.sourceEvent; ) e = t;
  return e;
}
function Di(e, t) {
  if (e = Sa(e), t === void 0 && (t = e.currentTarget), t) {
    var n = t.ownerSVGElement || t;
    if (n.createSVGPoint) {
      var i = n.createSVGPoint();
      return i.x = e.clientX, i.y = e.clientY, i = i.matrixTransform(t.getScreenCTM().inverse()), [i.x, i.y];
    }
    if (t.getBoundingClientRect) {
      var r = t.getBoundingClientRect();
      return [e.clientX - r.left - t.clientLeft, e.clientY - r.top - t.clientTop];
    }
  }
  return [e.pageX, e.pageY];
}
const Aa = { passive: !1 }, $e = { capture: !0, passive: !1 };
function $n(e) {
  e.stopImmediatePropagation();
}
function Te(e) {
  e.preventDefault(), e.stopImmediatePropagation();
}
function Ea(e) {
  var t = e.document.documentElement, n = wn(e).on("dragstart.drag", Te, $e);
  "onselectstart" in t ? n.on("selectstart.drag", Te, $e) : (t.__noselect = t.style.MozUserSelect, t.style.MozUserSelect = "none");
}
function xa(e, t) {
  var n = e.document.documentElement, i = wn(e).on("dragstart.drag", null);
  t && (i.on("click.drag", Te, $e), setTimeout(function() {
    i.on("click.drag", null);
  }, 0)), "onselectstart" in n ? i.on("selectstart.drag", null) : (n.style.MozUserSelect = n.__noselect, delete n.__noselect);
}
const fn = (e) => () => e;
function ai(e, {
  sourceEvent: t,
  subject: n,
  target: i,
  identifier: r,
  active: o,
  x: a,
  y: s,
  dx: c,
  dy: l,
  dispatch: f
}) {
  Object.defineProperties(this, {
    type: { value: e, enumerable: !0, configurable: !0 },
    sourceEvent: { value: t, enumerable: !0, configurable: !0 },
    subject: { value: n, enumerable: !0, configurable: !0 },
    target: { value: i, enumerable: !0, configurable: !0 },
    identifier: { value: r, enumerable: !0, configurable: !0 },
    active: { value: o, enumerable: !0, configurable: !0 },
    x: { value: a, enumerable: !0, configurable: !0 },
    y: { value: s, enumerable: !0, configurable: !0 },
    dx: { value: c, enumerable: !0, configurable: !0 },
    dy: { value: l, enumerable: !0, configurable: !0 },
    _: { value: f }
  });
}
ai.prototype.on = function() {
  var e = this._.on.apply(this._, arguments);
  return e === this._ ? this : e;
};
function Na(e) {
  return !e.ctrlKey && !e.button;
}
function Ra() {
  return this.parentNode;
}
function Ma(e, t) {
  return t ?? { x: e.x, y: e.y };
}
function Ia() {
  return navigator.maxTouchPoints || "ontouchstart" in this;
}
function Ca() {
  var e = Na, t = Ra, n = Ma, i = Ia, r = {}, o = di("start", "drag", "end"), a = 0, s, c, l, f, v = 0;
  function d(y) {
    y.on("mousedown.drag", m).filter(i).on("touchstart.drag", g).on("touchmove.drag", w, Aa).on("touchend.drag touchcancel.drag", _).style("touch-action", "none").style("-webkit-tap-highlight-color", "rgba(0,0,0,0)");
  }
  function m(y, C) {
    if (!(f || !e.call(this, y, C))) {
      var O = R(this, t.call(this, y, C), y, C, "mouse");
      O && (wn(y.view).on("mousemove.drag", S, $e).on("mouseup.drag", b, $e), Ea(y.view), $n(y), l = !1, s = y.clientX, c = y.clientY, O("start", y));
    }
  }
  function S(y) {
    if (Te(y), !l) {
      var C = y.clientX - s, O = y.clientY - c;
      l = C * C + O * O > v;
    }
    r.mouse("drag", y);
  }
  function b(y) {
    wn(y.view).on("mousemove.drag mouseup.drag", null), xa(y.view, l), Te(y), r.mouse("end", y);
  }
  function g(y, C) {
    if (e.call(this, y, C)) {
      var O = y.changedTouches, B = t.call(this, y, C), P = O.length, L, z;
      for (L = 0; L < P; ++L)
        (z = R(this, B, y, C, O[L].identifier, O[L])) && ($n(y), z("start", y, O[L]));
    }
  }
  function w(y) {
    var C = y.changedTouches, O = C.length, B, P;
    for (B = 0; B < O; ++B)
      (P = r[C[B].identifier]) && (Te(y), P("drag", y, C[B]));
  }
  function _(y) {
    var C = y.changedTouches, O = C.length, B, P;
    for (f && clearTimeout(f), f = setTimeout(function() {
      f = null;
    }, 500), B = 0; B < O; ++B)
      (P = r[C[B].identifier]) && ($n(y), P("end", y, C[B]));
  }
  function R(y, C, O, B, P, L) {
    var z = o.copy(), Y = Di(L || O, C), nt, N, D;
    if ((D = n.call(y, new ai("beforestart", {
      sourceEvent: O,
      target: d,
      identifier: P,
      active: a,
      x: Y[0],
      y: Y[1],
      dx: 0,
      dy: 0,
      dispatch: z
    }), B)) != null)
      return nt = D.x - Y[0] || 0, N = D.y - Y[1] || 0, function K(W, M, G) {
        var Z = Y, ct;
        switch (W) {
          case "start":
            r[P] = K, ct = a++;
            break;
          case "end":
            delete r[P], --a;
          // falls through
          case "drag":
            Y = Di(G || M, C), ct = a;
            break;
        }
        z.call(
          W,
          y,
          new ai(W, {
            sourceEvent: M,
            subject: D,
            target: d,
            identifier: P,
            active: ct,
            x: Y[0] + nt,
            y: Y[1] + N,
            dx: Y[0] - Z[0],
            dy: Y[1] - Z[1],
            dispatch: z
          }),
          B
        );
      };
  }
  return d.filter = function(y) {
    return arguments.length ? (e = typeof y == "function" ? y : fn(!!y), d) : e;
  }, d.container = function(y) {
    return arguments.length ? (t = typeof y == "function" ? y : fn(y), d) : t;
  }, d.subject = function(y) {
    return arguments.length ? (n = typeof y == "function" ? y : fn(y), d) : n;
  }, d.touchable = function(y) {
    return arguments.length ? (i = typeof y == "function" ? y : fn(!!y), d) : i;
  }, d.on = function() {
    var y = o.on.apply(o, arguments);
    return y === o ? d : y;
  }, d.clickDistance = function(y) {
    return arguments.length ? (v = (y = +y) * y, d) : Math.sqrt(v);
  }, d;
}
/*! @license DOMPurify 3.4.7 | (c) Cure53 and other contributors | Released under the Apache license 2.0 and Mozilla Public License 2.0 | github.com/cure53/DOMPurify/blob/3.4.7/LICENSE */
function Oi(e, t) {
  (t == null || t > e.length) && (t = e.length);
  for (var n = 0, i = Array(t); n < t; n++) i[n] = e[n];
  return i;
}
function Da(e) {
  if (Array.isArray(e)) return e;
}
function Oa(e, t) {
  var n = e == null ? null : typeof Symbol < "u" && e[Symbol.iterator] || e["@@iterator"];
  if (n != null) {
    var i, r, o, a, s = [], c = !0, l = !1;
    try {
      if (o = (n = n.call(e)).next, t !== 0) for (; !(c = (i = o.call(n)).done) && (s.push(i.value), s.length !== t); c = !0) ;
    } catch (f) {
      l = !0, r = f;
    } finally {
      try {
        if (!c && n.return != null && (a = n.return(), Object(a) !== a)) return;
      } finally {
        if (l) throw r;
      }
    }
    return s;
  }
}
function ka() {
  throw new TypeError(`Invalid attempt to destructure non-iterable instance.
In order to be iterable, non-array objects must have a [Symbol.iterator]() method.`);
}
function La(e, t) {
  return Da(e) || Oa(e, t) || Fa(e, t) || ka();
}
function Fa(e, t) {
  if (e) {
    if (typeof e == "string") return Oi(e, t);
    var n = {}.toString.call(e).slice(8, -1);
    return n === "Object" && e.constructor && (n = e.constructor.name), n === "Map" || n === "Set" ? Array.from(e) : n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n) ? Oi(e, t) : void 0;
  }
}
const Sr = Object.entries, ki = Object.setPrototypeOf, Pa = Object.isFrozen, za = Object.getPrototypeOf, Ba = Object.getOwnPropertyDescriptor;
let bt = Object.freeze, Ct = Object.seal, be = Object.create, Ar = typeof Reflect < "u" && Reflect, li = Ar.apply, ci = Ar.construct;
bt || (bt = function(t) {
  return t;
});
Ct || (Ct = function(t) {
  return t;
});
li || (li = function(t, n) {
  for (var i = arguments.length, r = new Array(i > 2 ? i - 2 : 0), o = 2; o < i; o++)
    r[o - 2] = arguments[o];
  return t.apply(n, r);
});
ci || (ci = function(t) {
  for (var n = arguments.length, i = new Array(n > 1 ? n - 1 : 0), r = 1; r < n; r++)
    i[r - 1] = arguments[r];
  return new t(...i);
});
const ye = ft(Array.prototype.forEach), Ga = ft(Array.prototype.lastIndexOf), Li = ft(Array.prototype.pop), _e = ft(Array.prototype.push), Ua = ft(Array.prototype.splice), _t = Array.isArray, We = ft(String.prototype.toLowerCase), Yn = ft(String.prototype.toString), Fi = ft(String.prototype.match), ve = ft(String.prototype.replace), Pi = ft(String.prototype.indexOf), ja = ft(String.prototype.trim), Ha = ft(Number.prototype.toString), Wa = ft(Boolean.prototype.toString), zi = typeof BigInt > "u" ? null : ft(BigInt.prototype.toString), Bi = typeof Symbol > "u" ? null : ft(Symbol.prototype.toString), ut = ft(Object.prototype.hasOwnProperty), Be = ft(Object.prototype.toString), pt = ft(RegExp.prototype.test), Ge = Ka(TypeError);
function ft(e) {
  return function(t) {
    t instanceof RegExp && (t.lastIndex = 0);
    for (var n = arguments.length, i = new Array(n > 1 ? n - 1 : 0), r = 1; r < n; r++)
      i[r - 1] = arguments[r];
    return li(e, t, i);
  };
}
function Ka(e) {
  return function() {
    for (var t = arguments.length, n = new Array(t), i = 0; i < t; i++)
      n[i] = arguments[i];
    return ci(e, n);
  };
}
function H(e, t) {
  let n = arguments.length > 2 && arguments[2] !== void 0 ? arguments[2] : We;
  if (ki && ki(e, null), !_t(t))
    return e;
  let i = t.length;
  for (; i--; ) {
    let r = t[i];
    if (typeof r == "string") {
      const o = n(r);
      o !== r && (Pa(t) || (t[i] = o), r = o);
    }
    e[r] = !0;
  }
  return e;
}
function Va(e) {
  for (let t = 0; t < e.length; t++)
    ut(e, t) || (e[t] = null);
  return e;
}
function mt(e) {
  const t = be(null);
  for (const i of Sr(e)) {
    var n = La(i, 2);
    const r = n[0], o = n[1];
    ut(e, r) && (_t(o) ? t[r] = Va(o) : o && typeof o == "object" && o.constructor === Object ? t[r] = mt(o) : t[r] = o);
  }
  return t;
}
function $a(e) {
  switch (typeof e) {
    case "string":
      return e;
    case "number":
      return Ha(e);
    case "boolean":
      return Wa(e);
    case "bigint":
      return zi ? zi(e) : "0";
    case "symbol":
      return Bi ? Bi(e) : "Symbol()";
    case "undefined":
      return Be(e);
    case "function":
    case "object": {
      if (e === null)
        return Be(e);
      const t = e, n = Ut(t, "toString");
      if (typeof n == "function") {
        const i = n(t);
        return typeof i == "string" ? i : Be(i);
      }
      return Be(e);
    }
    default:
      return Be(e);
  }
}
function Ut(e, t) {
  for (; e !== null; ) {
    const i = Ba(e, t);
    if (i) {
      if (i.get)
        return ft(i.get);
      if (typeof i.value == "function")
        return ft(i.value);
    }
    e = za(e);
  }
  function n() {
    return null;
  }
  return n;
}
function Ya(e) {
  try {
    return pt(e, ""), !0;
  } catch {
    return !1;
  }
}
const Gi = bt(["a", "abbr", "acronym", "address", "area", "article", "aside", "audio", "b", "bdi", "bdo", "big", "blink", "blockquote", "body", "br", "button", "canvas", "caption", "center", "cite", "code", "col", "colgroup", "content", "data", "datalist", "dd", "decorator", "del", "details", "dfn", "dialog", "dir", "div", "dl", "dt", "element", "em", "fieldset", "figcaption", "figure", "font", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "head", "header", "hgroup", "hr", "html", "i", "img", "input", "ins", "kbd", "label", "legend", "li", "main", "map", "mark", "marquee", "menu", "menuitem", "meter", "nav", "nobr", "ol", "optgroup", "option", "output", "p", "picture", "pre", "progress", "q", "rp", "rt", "ruby", "s", "samp", "search", "section", "select", "shadow", "slot", "small", "source", "spacer", "span", "strike", "strong", "style", "sub", "summary", "sup", "table", "tbody", "td", "template", "textarea", "tfoot", "th", "thead", "time", "tr", "track", "tt", "u", "ul", "var", "video", "wbr"]), qn = bt(["svg", "a", "altglyph", "altglyphdef", "altglyphitem", "animatecolor", "animatemotion", "animatetransform", "circle", "clippath", "defs", "desc", "ellipse", "enterkeyhint", "exportparts", "filter", "font", "g", "glyph", "glyphref", "hkern", "image", "inputmode", "line", "lineargradient", "marker", "mask", "metadata", "mpath", "part", "path", "pattern", "polygon", "polyline", "radialgradient", "rect", "stop", "style", "switch", "symbol", "text", "textpath", "title", "tref", "tspan", "view", "vkern"]), Xn = bt(["feBlend", "feColorMatrix", "feComponentTransfer", "feComposite", "feConvolveMatrix", "feDiffuseLighting", "feDisplacementMap", "feDistantLight", "feDropShadow", "feFlood", "feFuncA", "feFuncB", "feFuncG", "feFuncR", "feGaussianBlur", "feImage", "feMerge", "feMergeNode", "feMorphology", "feOffset", "fePointLight", "feSpecularLighting", "feSpotLight", "feTile", "feTurbulence"]), qa = bt(["animate", "color-profile", "cursor", "discard", "font-face", "font-face-format", "font-face-name", "font-face-src", "font-face-uri", "foreignobject", "hatch", "hatchpath", "mesh", "meshgradient", "meshpatch", "meshrow", "missing-glyph", "script", "set", "solidcolor", "unknown", "use"]), Zn = bt(["math", "menclose", "merror", "mfenced", "mfrac", "mglyph", "mi", "mlabeledtr", "mmultiscripts", "mn", "mo", "mover", "mpadded", "mphantom", "mroot", "mrow", "ms", "mspace", "msqrt", "mstyle", "msub", "msup", "msubsup", "mtable", "mtd", "mtext", "mtr", "munder", "munderover", "mprescripts"]), Xa = bt(["maction", "maligngroup", "malignmark", "mlongdiv", "mscarries", "mscarry", "msgroup", "mstack", "msline", "msrow", "semantics", "annotation", "annotation-xml", "mprescripts", "none"]), Ui = bt(["#text"]), ji = bt(["accept", "action", "align", "alt", "autocapitalize", "autocomplete", "autopictureinpicture", "autoplay", "background", "bgcolor", "border", "capture", "cellpadding", "cellspacing", "checked", "cite", "class", "clear", "color", "cols", "colspan", "command", "commandfor", "controls", "controlslist", "coords", "crossorigin", "datetime", "decoding", "default", "dir", "disabled", "disablepictureinpicture", "disableremoteplayback", "download", "draggable", "enctype", "enterkeyhint", "exportparts", "face", "for", "headers", "height", "hidden", "high", "href", "hreflang", "id", "inert", "inputmode", "integrity", "ismap", "kind", "label", "lang", "list", "loading", "loop", "low", "max", "maxlength", "media", "method", "min", "minlength", "multiple", "muted", "name", "nonce", "noshade", "novalidate", "nowrap", "open", "optimum", "part", "pattern", "placeholder", "playsinline", "popover", "popovertarget", "popovertargetaction", "poster", "preload", "pubdate", "radiogroup", "readonly", "rel", "required", "rev", "reversed", "role", "rows", "rowspan", "spellcheck", "scope", "selected", "shape", "size", "sizes", "slot", "span", "srclang", "start", "src", "srcset", "step", "style", "summary", "tabindex", "title", "translate", "type", "usemap", "valign", "value", "width", "wrap", "xmlns"]), Qn = bt(["accent-height", "accumulate", "additive", "alignment-baseline", "amplitude", "ascent", "attributename", "attributetype", "azimuth", "basefrequency", "baseline-shift", "begin", "bias", "by", "class", "clip", "clippathunits", "clip-path", "clip-rule", "color", "color-interpolation", "color-interpolation-filters", "color-profile", "color-rendering", "cx", "cy", "d", "dx", "dy", "diffuseconstant", "direction", "display", "divisor", "dur", "edgemode", "elevation", "end", "exponent", "fill", "fill-opacity", "fill-rule", "filter", "filterunits", "flood-color", "flood-opacity", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight", "fx", "fy", "g1", "g2", "glyph-name", "glyphref", "gradientunits", "gradienttransform", "height", "href", "id", "image-rendering", "in", "in2", "intercept", "k", "k1", "k2", "k3", "k4", "kerning", "keypoints", "keysplines", "keytimes", "lang", "lengthadjust", "letter-spacing", "kernelmatrix", "kernelunitlength", "lighting-color", "local", "marker-end", "marker-mid", "marker-start", "markerheight", "markerunits", "markerwidth", "maskcontentunits", "maskunits", "max", "mask", "mask-type", "media", "method", "mode", "min", "name", "numoctaves", "offset", "operator", "opacity", "order", "orient", "orientation", "origin", "overflow", "paint-order", "path", "pathlength", "patterncontentunits", "patterntransform", "patternunits", "points", "preservealpha", "preserveaspectratio", "primitiveunits", "r", "rx", "ry", "radius", "refx", "refy", "repeatcount", "repeatdur", "restart", "result", "rotate", "scale", "seed", "shape-rendering", "slope", "specularconstant", "specularexponent", "spreadmethod", "startoffset", "stddeviation", "stitchtiles", "stop-color", "stop-opacity", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke", "stroke-width", "style", "surfacescale", "systemlanguage", "tabindex", "tablevalues", "targetx", "targety", "transform", "transform-origin", "text-anchor", "text-decoration", "text-rendering", "textlength", "type", "u1", "u2", "unicode", "values", "viewbox", "visibility", "version", "vert-adv-y", "vert-origin-x", "vert-origin-y", "width", "word-spacing", "wrap", "writing-mode", "xchannelselector", "ychannelselector", "x", "x1", "x2", "xmlns", "y", "y1", "y2", "z", "zoomandpan"]), Hi = bt(["accent", "accentunder", "align", "bevelled", "close", "columnalign", "columnlines", "columnspacing", "columnspan", "denomalign", "depth", "dir", "display", "displaystyle", "encoding", "fence", "frame", "height", "href", "id", "largeop", "length", "linethickness", "lquote", "lspace", "mathbackground", "mathcolor", "mathsize", "mathvariant", "maxsize", "minsize", "movablelimits", "notation", "numalign", "open", "rowalign", "rowlines", "rowspacing", "rowspan", "rspace", "rquote", "scriptlevel", "scriptminsize", "scriptsizemultiplier", "selection", "separator", "separators", "stretchy", "subscriptshift", "supscriptshift", "symmetric", "voffset", "width", "xmlns"]), dn = bt(["xlink:href", "xml:id", "xlink:title", "xml:space", "xmlns:xlink"]), Za = Ct(/{{[\w\W]*|^[\w\W]*}}/g), Qa = Ct(/<%[\w\W]*|^[\w\W]*%>/g), Ja = Ct(/\${[\w\W]*/g), tl = Ct(/^data-[\-\w.\u00B7-\uFFFF]+$/), el = Ct(/^aria-[\-\w]+$/), Wi = Ct(
  /^(?:(?:(?:f|ht)tps?|mailto|tel|callto|sms|cid|xmpp|matrix):|[^a-z]|[a-z+.\-]+(?:[^a-z+.\-:]|$))/i
  // eslint-disable-line no-useless-escape
), nl = Ct(/^(?:\w+script|data):/i), il = Ct(
  /[\u0000-\u0020\u00A0\u1680\u180E\u2000-\u2029\u205F\u3000]/g
  // eslint-disable-line no-control-regex
), rl = Ct(/^html$/i), ol = Ct(/^[a-z][.\w]*(-[.\w]+)+$/i), Gt = {
  element: 1,
  attribute: 2,
  text: 3,
  cdataSection: 4,
  entityReference: 5,
  // Deprecated
  entityNode: 6,
  // Deprecated
  progressingInstruction: 7,
  comment: 8,
  document: 9,
  documentType: 10,
  documentFragment: 11,
  notation: 12
  // Deprecated
}, sl = function() {
  return typeof window > "u" ? null : window;
}, al = function(t, n) {
  if (typeof t != "object" || typeof t.createPolicy != "function")
    return null;
  let i = null;
  const r = "data-tt-policy-suffix";
  n && n.hasAttribute(r) && (i = n.getAttribute(r));
  const o = "dompurify" + (i ? "#" + i : "");
  try {
    return t.createPolicy(o, {
      createHTML(a) {
        return a;
      },
      createScriptURL(a) {
        return a;
      }
    });
  } catch {
    return console.warn("TrustedTypes policy " + o + " could not be created."), null;
  }
}, Ki = function() {
  return {
    afterSanitizeAttributes: [],
    afterSanitizeElements: [],
    afterSanitizeShadowDOM: [],
    beforeSanitizeAttributes: [],
    beforeSanitizeElements: [],
    beforeSanitizeShadowDOM: [],
    uponSanitizeAttribute: [],
    uponSanitizeElement: [],
    uponSanitizeShadowNode: []
  };
};
function Er() {
  let e = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : sl();
  const t = (k) => Er(k);
  if (t.version = "3.4.7", t.removed = [], !e || !e.document || e.document.nodeType !== Gt.document || !e.Element)
    return t.isSupported = !1, t;
  let n = e.document;
  const i = n, r = i.currentScript;
  e.DocumentFragment;
  const o = e.HTMLTemplateElement, a = e.Node, s = e.Element, c = e.NodeFilter, l = e.NamedNodeMap;
  l === void 0 && (e.NamedNodeMap || e.MozNamedAttrMap), e.HTMLFormElement;
  const f = e.DOMParser, v = e.trustedTypes, d = s.prototype, m = Ut(d, "cloneNode"), S = Ut(d, "remove"), b = Ut(d, "nextSibling"), g = Ut(d, "childNodes"), w = Ut(d, "parentNode"), _ = Ut(d, "shadowRoot"), R = Ut(d, "attributes"), y = a && a.prototype ? Ut(a.prototype, "nodeType") : null, C = a && a.prototype ? Ut(a.prototype, "nodeName") : null;
  if (typeof o == "function") {
    const k = n.createElement("template");
    k.content && k.content.ownerDocument && (n = k.content.ownerDocument);
  }
  let O, B = "";
  const P = n, L = P.implementation, z = P.createNodeIterator, Y = P.createDocumentFragment, nt = P.getElementsByTagName, N = i.importNode;
  let D = Ki();
  t.isSupported = typeof Sr == "function" && typeof w == "function" && L && L.createHTMLDocument !== void 0;
  const K = Za, W = Qa, M = Ja, G = tl, Z = el, ct = nl, gt = il, it = ol;
  let j = Wi, X = null;
  const Xt = H({}, [...Gi, ...qn, ...Xn, ...Zn, ...Ui]);
  let J = null;
  const Zt = H({}, [...ji, ...Qn, ...Hi, ...dn]);
  let U = Object.seal(be(null, {
    tagNameCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    },
    attributeNameCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    },
    allowCustomizedBuiltInElements: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: !1
    }
  })), yt = null, Qt = null;
  const Dt = Object.seal(be(null, {
    tagCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    },
    attributeCheck: {
      writable: !0,
      configurable: !1,
      enumerable: !0,
      value: null
    }
  }));
  let Ae = !0, Ee = !0, Xe = !1, Ze = !0, Ft = !1, oe = !0, jt = !1, xe = !1, Jt = !1, Pt = !1, Ht = !1, St = !1, Ne = !0, Re = !1;
  const Qe = "user-content-";
  let Me = !0, zt = !1, Wt = {}, At = null;
  const fe = H({}, ["annotation-xml", "audio", "colgroup", "desc", "foreignobject", "head", "iframe", "math", "mi", "mn", "mo", "ms", "mtext", "noembed", "noframes", "noscript", "plaintext", "script", "style", "svg", "template", "thead", "title", "video", "xmp"]);
  let Ie = null;
  const Je = H({}, ["audio", "video", "img", "source", "image", "track"]);
  let Ce = null;
  const Bt = H({}, ["alt", "class", "for", "id", "label", "name", "pattern", "placeholder", "role", "summary", "title", "value", "style", "xmlns"]), Kt = "http://www.w3.org/1998/Math/MathML", de = "http://www.w3.org/2000/svg", Et = "http://www.w3.org/1999/xhtml";
  let te = Et, pe = !1, Vt = null;
  const On = H({}, [Kt, de, Et], Yn);
  let Ot = H({}, ["mi", "mo", "mn", "ms", "mtext"]), De = H({}, ["annotation-xml"]);
  const kn = H({}, ["title", "style", "font", "a", "script"]);
  let se = null;
  const Ln = ["application/xhtml+xml", "text/html"], Fn = "text/html";
  let tt = null, ee = null;
  const Pn = n.createElement("form"), tn = function(h) {
    return h instanceof RegExp || h instanceof Function;
  }, Oe = function() {
    let h = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : {};
    if (ee && ee === h)
      return;
    (!h || typeof h != "object") && (h = {}), h = mt(h), se = // eslint-disable-next-line unicorn/prefer-includes
    Ln.indexOf(h.PARSER_MEDIA_TYPE) === -1 ? Fn : h.PARSER_MEDIA_TYPE, tt = se === "application/xhtml+xml" ? Yn : We, X = ut(h, "ALLOWED_TAGS") && _t(h.ALLOWED_TAGS) ? H({}, h.ALLOWED_TAGS, tt) : Xt, J = ut(h, "ALLOWED_ATTR") && _t(h.ALLOWED_ATTR) ? H({}, h.ALLOWED_ATTR, tt) : Zt, Vt = ut(h, "ALLOWED_NAMESPACES") && _t(h.ALLOWED_NAMESPACES) ? H({}, h.ALLOWED_NAMESPACES, Yn) : On, Ce = ut(h, "ADD_URI_SAFE_ATTR") && _t(h.ADD_URI_SAFE_ATTR) ? H(mt(Bt), h.ADD_URI_SAFE_ATTR, tt) : Bt, Ie = ut(h, "ADD_DATA_URI_TAGS") && _t(h.ADD_DATA_URI_TAGS) ? H(mt(Je), h.ADD_DATA_URI_TAGS, tt) : Je, At = ut(h, "FORBID_CONTENTS") && _t(h.FORBID_CONTENTS) ? H({}, h.FORBID_CONTENTS, tt) : fe, yt = ut(h, "FORBID_TAGS") && _t(h.FORBID_TAGS) ? H({}, h.FORBID_TAGS, tt) : mt({}), Qt = ut(h, "FORBID_ATTR") && _t(h.FORBID_ATTR) ? H({}, h.FORBID_ATTR, tt) : mt({}), Wt = ut(h, "USE_PROFILES") ? h.USE_PROFILES && typeof h.USE_PROFILES == "object" ? mt(h.USE_PROFILES) : h.USE_PROFILES : !1, Ae = h.ALLOW_ARIA_ATTR !== !1, Ee = h.ALLOW_DATA_ATTR !== !1, Xe = h.ALLOW_UNKNOWN_PROTOCOLS || !1, Ze = h.ALLOW_SELF_CLOSE_IN_ATTR !== !1, Ft = h.SAFE_FOR_TEMPLATES || !1, oe = h.SAFE_FOR_XML !== !1, jt = h.WHOLE_DOCUMENT || !1, Pt = h.RETURN_DOM || !1, Ht = h.RETURN_DOM_FRAGMENT || !1, St = h.RETURN_TRUSTED_TYPE || !1, Jt = h.FORCE_BODY || !1, Ne = h.SANITIZE_DOM !== !1, Re = h.SANITIZE_NAMED_PROPS || !1, Me = h.KEEP_CONTENT !== !1, zt = h.IN_PLACE || !1, j = Ya(h.ALLOWED_URI_REGEXP) ? h.ALLOWED_URI_REGEXP : Wi, te = typeof h.NAMESPACE == "string" ? h.NAMESPACE : Et, Ot = ut(h, "MATHML_TEXT_INTEGRATION_POINTS") && h.MATHML_TEXT_INTEGRATION_POINTS && typeof h.MATHML_TEXT_INTEGRATION_POINTS == "object" ? mt(h.MATHML_TEXT_INTEGRATION_POINTS) : H({}, ["mi", "mo", "mn", "ms", "mtext"]), De = ut(h, "HTML_INTEGRATION_POINTS") && h.HTML_INTEGRATION_POINTS && typeof h.HTML_INTEGRATION_POINTS == "object" ? mt(h.HTML_INTEGRATION_POINTS) : H({}, ["annotation-xml"]);
    const A = ut(h, "CUSTOM_ELEMENT_HANDLING") && h.CUSTOM_ELEMENT_HANDLING && typeof h.CUSTOM_ELEMENT_HANDLING == "object" ? mt(h.CUSTOM_ELEMENT_HANDLING) : be(null);
    if (U = be(null), ut(A, "tagNameCheck") && tn(A.tagNameCheck) && (U.tagNameCheck = A.tagNameCheck), ut(A, "attributeNameCheck") && tn(A.attributeNameCheck) && (U.attributeNameCheck = A.attributeNameCheck), ut(A, "allowCustomizedBuiltInElements") && typeof A.allowCustomizedBuiltInElements == "boolean" && (U.allowCustomizedBuiltInElements = A.allowCustomizedBuiltInElements), Ft && (Ee = !1), Ht && (Pt = !0), Wt && (X = H({}, Ui), J = be(null), Wt.html === !0 && (H(X, Gi), H(J, ji)), Wt.svg === !0 && (H(X, qn), H(J, Qn), H(J, dn)), Wt.svgFilters === !0 && (H(X, Xn), H(J, Qn), H(J, dn)), Wt.mathMl === !0 && (H(X, Zn), H(J, Hi), H(J, dn))), Dt.tagCheck = null, Dt.attributeCheck = null, ut(h, "ADD_TAGS") && (typeof h.ADD_TAGS == "function" ? Dt.tagCheck = h.ADD_TAGS : _t(h.ADD_TAGS) && (X === Xt && (X = mt(X)), H(X, h.ADD_TAGS, tt))), ut(h, "ADD_ATTR") && (typeof h.ADD_ATTR == "function" ? Dt.attributeCheck = h.ADD_ATTR : _t(h.ADD_ATTR) && (J === Zt && (J = mt(J)), H(J, h.ADD_ATTR, tt))), ut(h, "ADD_URI_SAFE_ATTR") && _t(h.ADD_URI_SAFE_ATTR) && H(Ce, h.ADD_URI_SAFE_ATTR, tt), ut(h, "FORBID_CONTENTS") && _t(h.FORBID_CONTENTS) && (At === fe && (At = mt(At)), H(At, h.FORBID_CONTENTS, tt)), ut(h, "ADD_FORBID_CONTENTS") && _t(h.ADD_FORBID_CONTENTS) && (At === fe && (At = mt(At)), H(At, h.ADD_FORBID_CONTENTS, tt)), Me && (X["#text"] = !0), jt && H(X, ["html", "head", "body"]), X.table && (H(X, ["tbody"]), delete yt.tbody), h.TRUSTED_TYPES_POLICY) {
      if (typeof h.TRUSTED_TYPES_POLICY.createHTML != "function")
        throw Ge('TRUSTED_TYPES_POLICY configuration option must provide a "createHTML" hook.');
      if (typeof h.TRUSTED_TYPES_POLICY.createScriptURL != "function")
        throw Ge('TRUSTED_TYPES_POLICY configuration option must provide a "createScriptURL" hook.');
      O = h.TRUSTED_TYPES_POLICY, B = O.createHTML("");
    } else
      O === void 0 && (O = al(v, r)), O !== null && typeof B == "string" && (B = O.createHTML(""));
    (D.uponSanitizeElement.length > 0 || D.uponSanitizeAttribute.length > 0) && X === Xt && (X = mt(X)), D.uponSanitizeAttribute.length > 0 && J === Zt && (J = mt(J)), bt && bt(h), ee = h;
  }, en = H({}, [...qn, ...Xn, ...qa]), $t = H({}, [...Zn, ...Xa]), zn = function(h) {
    let A = w(h);
    (!A || !A.tagName) && (A = {
      namespaceURI: te,
      tagName: "template"
    });
    const I = We(h.tagName), Q = We(A.tagName);
    return Vt[h.namespaceURI] ? h.namespaceURI === de ? A.namespaceURI === Et ? I === "svg" : A.namespaceURI === Kt ? I === "svg" && (Q === "annotation-xml" || Ot[Q]) : !!en[I] : h.namespaceURI === Kt ? A.namespaceURI === Et ? I === "math" : A.namespaceURI === de ? I === "math" && De[Q] : !!$t[I] : h.namespaceURI === Et ? A.namespaceURI === de && !De[Q] || A.namespaceURI === Kt && !Ot[Q] ? !1 : !$t[I] && (kn[I] || !en[I]) : !!(se === "application/xhtml+xml" && Vt[h.namespaceURI]) : !1;
  }, xt = function(h) {
    _e(t.removed, {
      element: h
    });
    try {
      w(h).removeChild(h);
    } catch {
      S(h);
    }
  }, Yt = function(h, A) {
    try {
      _e(t.removed, {
        attribute: A.getAttributeNode(h),
        from: A
      });
    } catch {
      _e(t.removed, {
        attribute: null,
        from: A
      });
    }
    if (A.removeAttribute(h), h === "is")
      if (Pt || Ht)
        try {
          xt(A);
        } catch {
        }
      else
        try {
          A.setAttribute(h, "");
        } catch {
        }
  }, nn = function(h) {
    let A = null, I = null;
    if (Jt)
      h = "<remove></remove>" + h;
    else {
      const rt = Fi(h, /^[\r\n\t ]+/);
      I = rt && rt[0];
    }
    se === "application/xhtml+xml" && te === Et && (h = '<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body>' + h + "</body></html>");
    const Q = O ? O.createHTML(h) : h;
    if (te === Et)
      try {
        A = new f().parseFromString(Q, se);
      } catch {
      }
    if (!A || !A.documentElement) {
      A = L.createDocument(te, "template", null);
      try {
        A.documentElement.innerHTML = pe ? B : Q;
      } catch {
      }
    }
    const V = A.body || A.documentElement;
    return h && I && V.insertBefore(n.createTextNode(I), V.childNodes[0] || null), te === Et ? nt.call(A, jt ? "html" : "body")[0] : jt ? A.documentElement : V;
  }, rn = function(h) {
    return z.call(
      h.ownerDocument || h,
      h,
      // eslint-disable-next-line no-bitwise
      c.SHOW_ELEMENT | c.SHOW_COMMENT | c.SHOW_TEXT | c.SHOW_PROCESSING_INSTRUCTION | c.SHOW_CDATA_SECTION,
      null
    );
  }, qt = function(h) {
    h.normalize();
    const A = z.call(
      h.ownerDocument || h,
      h,
      // eslint-disable-next-line no-bitwise
      c.SHOW_TEXT | c.SHOW_COMMENT | c.SHOW_CDATA_SECTION | c.SHOW_PROCESSING_INSTRUCTION,
      null
    );
    let I = A.nextNode();
    for (; I; ) {
      let Q = I.data;
      ye([K, W, M], (V) => {
        Q = ve(Q, V, " ");
      }), I.data = Q, I = A.nextNode();
    }
  }, ge = function(h) {
    const A = C ? C(h) : null;
    return typeof A != "string" || tt(A) !== "form" ? !1 : typeof h.nodeName != "string" || typeof h.textContent != "string" || typeof h.removeChild != "function" || // Realm-safe NamedNodeMap detection: equality against the cached
    // prototype getter. Clobbered .attributes (e.g. <input name="attributes">)
    // makes the direct read diverge from the cached read; a clean form
    // (same-realm OR foreign-realm) has both reads pointing at the same
    // canonical NamedNodeMap.
    h.attributes !== R(h) || typeof h.removeAttribute != "function" || typeof h.setAttribute != "function" || typeof h.namespaceURI != "string" || typeof h.insertBefore != "function" || typeof h.hasChildNodes != "function" || // NodeType clobbering probe. Cached Node.prototype.nodeType getter
    // returns the integer 1 for any Element regardless of realm; direct
    // read on a clobbered form (e.g. <input name="nodeType">) returns
    // the named child element. Cheap addition — nodeType is read from
    // an internal slot, no serialization cost — and removes a residual
    // clobbering surface used by several mXSS / PI / comment branches
    // in _sanitizeElements that compare currentNode.nodeType directly.
    h.nodeType !== y(h) || // HTMLFormElement has [LegacyOverrideBuiltIns]: a descendant named
    // "childNodes" shadows the prototype getter. Direct reads of
    // form.childNodes from a clobbered form return the named child
    // instead of the real NodeList, so any walk that reads it directly
    // skips the form's real children. Compare the direct read to the
    // cached Node.prototype getter — when the form's named-property
    // getter intercepts the read, the two values differ and we flag
    // the form. This catches every clobbering child type (input,
    // select, etc.) regardless of whether the named child happens to
    // carry a numeric .length, which a typeof-based probe would miss
    // (e.g. HTMLSelectElement.length is a defined unsigned-long).
    h.childNodes !== g(h);
  }, ae = function(h) {
    if (!y || typeof h != "object" || h === null)
      return !1;
    try {
      return y(h) === Gt.documentFragment;
    } catch {
      return !1;
    }
  }, me = function(h) {
    if (!y || typeof h != "object" || h === null)
      return !1;
    try {
      return typeof y(h) == "number";
    } catch {
      return !1;
    }
  };
  function kt(k, h, A) {
    ye(k, (I) => {
      I.call(t, h, A, ee);
    });
  }
  const on = function(h) {
    let A = null;
    if (kt(D.beforeSanitizeElements, h, null), ge(h))
      return xt(h), !0;
    const I = tt(h.nodeName);
    if (kt(D.uponSanitizeElement, h, {
      tagName: I,
      allowedTags: X
    }), oe && h.hasChildNodes() && !me(h.firstElementChild) && pt(/<[/\w!]/g, h.innerHTML) && pt(/<[/\w!]/g, h.textContent) || oe && h.namespaceURI === Et && I === "style" && me(h.firstElementChild) || h.nodeType === Gt.progressingInstruction || oe && h.nodeType === Gt.comment && pt(/<[/\w]/g, h.data))
      return xt(h), !0;
    if (yt[I] || !(Dt.tagCheck instanceof Function && Dt.tagCheck(I)) && !X[I]) {
      if (!yt[I] && an(I) && (U.tagNameCheck instanceof RegExp && pt(U.tagNameCheck, I) || U.tagNameCheck instanceof Function && U.tagNameCheck(I)))
        return !1;
      if (Me && !At[I]) {
        const V = w(h), rt = g(h);
        if (rt && V) {
          const wt = rt.length;
          for (let Mt = wt - 1; Mt >= 0; --Mt) {
            const Nt = m(rt[Mt], !0);
            V.insertBefore(Nt, b(h));
          }
        }
      }
      return xt(h), !0;
    }
    return (y ? y(h) : h.nodeType) === Gt.element && !zn(h) || (I === "noscript" || I === "noembed" || I === "noframes") && pt(/<\/no(script|embed|frames)/i, h.innerHTML) ? (xt(h), !0) : (Ft && h.nodeType === Gt.text && (A = h.textContent, ye([K, W, M], (V) => {
      A = ve(A, V, " ");
    }), h.textContent !== A && (_e(t.removed, {
      element: h.cloneNode()
    }), h.textContent = A)), kt(D.afterSanitizeElements, h, null), !1);
  }, sn = function(h, A, I) {
    if (Qt[A] || Ne && (A === "id" || A === "name") && (I in n || I in Pn))
      return !1;
    const Q = J[A] || Dt.attributeCheck instanceof Function && Dt.attributeCheck(A, h);
    if (!(Ee && !Qt[A] && pt(G, A))) {
      if (!(Ae && pt(Z, A))) {
        if (!Q || Qt[A]) {
          if (
            // First condition does a very basic check if a) it's basically a valid custom element tagname AND
            // b) if the tagName passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.tagNameCheck
            // and c) if the attribute name passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.attributeNameCheck
            !(an(h) && (U.tagNameCheck instanceof RegExp && pt(U.tagNameCheck, h) || U.tagNameCheck instanceof Function && U.tagNameCheck(h)) && (U.attributeNameCheck instanceof RegExp && pt(U.attributeNameCheck, A) || U.attributeNameCheck instanceof Function && U.attributeNameCheck(A, h)) || // Alternative, second condition checks if it's an `is`-attribute, AND
            // the value passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.tagNameCheck
            A === "is" && U.allowCustomizedBuiltInElements && (U.tagNameCheck instanceof RegExp && pt(U.tagNameCheck, I) || U.tagNameCheck instanceof Function && U.tagNameCheck(I)))
          ) return !1;
        } else if (!Ce[A]) {
          if (!pt(j, ve(I, gt, ""))) {
            if (!((A === "src" || A === "xlink:href" || A === "href") && h !== "script" && Pi(I, "data:") === 0 && Ie[h])) {
              if (!(Xe && !pt(ct, ve(I, gt, "")))) {
                if (I)
                  return !1;
              }
            }
          }
        }
      }
    }
    return !0;
  }, ke = H({}, ["annotation-xml", "color-profile", "font-face", "font-face-format", "font-face-name", "font-face-src", "font-face-uri", "missing-glyph"]), an = function(h) {
    return !ke[We(h)] && pt(it, h);
  }, le = function(h) {
    kt(D.beforeSanitizeAttributes, h, null);
    const A = h.attributes;
    if (!A || ge(h))
      return;
    const I = {
      attrName: "",
      attrValue: "",
      keepAttr: !0,
      allowedAttributes: J,
      forceKeepAttr: void 0
    };
    let Q = A.length;
    for (; Q--; ) {
      const V = A[Q], rt = V.name, wt = V.namespaceURI, Mt = V.value, Nt = tt(rt), Le = Mt;
      let ht = rt === "value" ? Le : ja(Le);
      if (I.attrName = Nt, I.attrValue = ht, I.keepAttr = !0, I.forceKeepAttr = void 0, kt(D.uponSanitizeAttribute, h, I), ht = I.attrValue, Re && (Nt === "id" || Nt === "name") && Pi(ht, Qe) !== 0 && (Yt(rt, h), ht = Qe + ht), oe && pt(/((--!?|])>)|<\/(style|script|title|xmp|textarea|noscript|iframe|noembed|noframes)/i, ht)) {
        Yt(rt, h);
        continue;
      }
      if (Nt === "attributename" && Fi(ht, "href")) {
        Yt(rt, h);
        continue;
      }
      if (I.forceKeepAttr)
        continue;
      if (!I.keepAttr) {
        Yt(rt, h);
        continue;
      }
      if (!Ze && pt(/\/>/i, ht)) {
        Yt(rt, h);
        continue;
      }
      Ft && ye([K, W, M], (cn) => {
        ht = ve(ht, cn, " ");
      });
      const ln = tt(h.nodeName);
      if (!sn(ln, Nt, ht)) {
        Yt(rt, h);
        continue;
      }
      if (O && typeof v == "object" && typeof v.getAttributeType == "function" && !wt)
        switch (v.getAttributeType(ln, Nt)) {
          case "TrustedHTML": {
            ht = O.createHTML(ht);
            break;
          }
          case "TrustedScriptURL": {
            ht = O.createScriptURL(ht);
            break;
          }
        }
      if (ht !== Le)
        try {
          wt ? h.setAttributeNS(wt, rt, ht) : h.setAttribute(rt, ht), ge(h) ? xt(h) : Li(t.removed);
        } catch {
          Yt(rt, h);
        }
    }
    kt(D.afterSanitizeAttributes, h, null);
  }, ne = function(h) {
    let A = null;
    const I = rn(h);
    for (kt(D.beforeSanitizeShadowDOM, h, null); A = I.nextNode(); )
      if (kt(D.uponSanitizeShadowNode, A, null), on(A), le(A), ae(A.content) && ne(A.content), (y ? y(A) : A.nodeType) === Gt.element) {
        const V = _ ? _(A) : A.shadowRoot;
        ae(V) && (ie(V), ne(V));
      }
    kt(D.afterSanitizeShadowDOM, h, null);
  }, ie = function(h) {
    const A = y ? y(h) : h.nodeType;
    if (A === Gt.element) {
      const V = _ ? _(h) : h.shadowRoot;
      ae(V) && (ie(V), ne(V));
    }
    const I = g ? g(h) : h.childNodes;
    if (!I)
      return;
    const Q = [];
    ye(I, (V) => {
      _e(Q, V);
    });
    for (const V of Q)
      ie(V);
    if (A === Gt.element) {
      const V = C ? C(h) : null;
      if (typeof V == "string" && tt(V) === "template") {
        const rt = h.content;
        ae(rt) && ie(rt);
      }
    }
  };
  return t.sanitize = function(k) {
    let h = arguments.length > 1 && arguments[1] !== void 0 ? arguments[1] : {}, A = null, I = null, Q = null, V = null;
    if (pe = !k, pe && (k = "<!-->"), typeof k != "string" && !me(k) && (k = $a(k), typeof k != "string"))
      throw Ge("dirty is not a string, aborting");
    if (!t.isSupported)
      return k;
    if (xe || Oe(h), t.removed = [], typeof k == "string" && (zt = !1), zt) {
      const Mt = C ? C(k) : k.nodeName;
      if (typeof Mt == "string") {
        const Nt = tt(Mt);
        if (!X[Nt] || yt[Nt])
          throw Ge("root node is forbidden and cannot be sanitized in-place");
      }
      if (ge(k))
        throw Ge("root node is clobbered and cannot be sanitized in-place");
      ie(k);
    } else if (me(k))
      A = nn("<!---->"), I = A.ownerDocument.importNode(k, !0), I.nodeType === Gt.element && I.nodeName === "BODY" || I.nodeName === "HTML" ? A = I : A.appendChild(I), ie(I);
    else {
      if (!Pt && !Ft && !jt && // eslint-disable-next-line unicorn/prefer-includes
      k.indexOf("<") === -1)
        return O && St ? O.createHTML(k) : k;
      if (A = nn(k), !A)
        return Pt ? null : St ? B : "";
    }
    A && Jt && xt(A.firstChild);
    const rt = rn(zt ? k : A);
    for (; Q = rt.nextNode(); )
      on(Q), le(Q), ae(Q.content) && ne(Q.content);
    if (zt)
      return Ft && qt(k), k;
    if (Pt) {
      if (Ft && qt(A), Ht)
        for (V = Y.call(A.ownerDocument); A.firstChild; )
          V.appendChild(A.firstChild);
      else
        V = A;
      return (J.shadowroot || J.shadowrootmode) && (V = N.call(i, V, !0)), V;
    }
    let wt = jt ? A.outerHTML : A.innerHTML;
    return jt && X["!doctype"] && A.ownerDocument && A.ownerDocument.doctype && A.ownerDocument.doctype.name && pt(rl, A.ownerDocument.doctype.name) && (wt = "<!DOCTYPE " + A.ownerDocument.doctype.name + `>
` + wt), Ft && ye([K, W, M], (Mt) => {
      wt = ve(wt, Mt, " ");
    }), O && St ? O.createHTML(wt) : wt;
  }, t.setConfig = function() {
    let k = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : {};
    Oe(k), xe = !0;
  }, t.clearConfig = function() {
    ee = null, xe = !1;
  }, t.isValidAttribute = function(k, h, A) {
    ee || Oe({});
    const I = tt(k), Q = tt(h);
    return sn(I, Q, A);
  }, t.addHook = function(k, h) {
    typeof h == "function" && _e(D[k], h);
  }, t.removeHook = function(k, h) {
    if (h !== void 0) {
      const A = Ga(D[k], h);
      return A === -1 ? void 0 : Ua(D[k], A, 1)[0];
    }
    return Li(D[k]);
  }, t.removeHooks = function(k) {
    D[k] = [];
  }, t.removeAllHooks = function() {
    D = Ki();
  }, t;
}
Er();
function xr(e = 8, t = "id-") {
  const n = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", i = n + "0123456789-_";
  let r = n.charAt(Math.floor(Math.random() * n.length));
  for (let o = 1; o < e; o++)
    r += i.charAt(Math.floor(Math.random() * i.length));
  return `${t}${r}`;
}
function ll(e, t, n, i) {
  const r = Math.max(Math.abs(n) / e, Math.abs(i) / t);
  return r === 0 ? e : 1 / r;
}
function Sn(e, t = /* @__PURE__ */ new WeakSet()) {
  if (typeof e == "function") return;
  if (e === null || typeof e != "object") return e;
  const n = e;
  if (t.has(n)) return;
  if (t.add(n), Array.isArray(e))
    return e.map((r) => Sn(r, t));
  if (Object.getPrototypeOf(e) !== Object.prototype) return e;
  const i = {};
  for (const [r, o] of Object.entries(e))
    typeof o != "function" && (i[r] = Sn(o, t));
  return i;
}
let Nr = class Rr {
  /**
   * Create a new Node instance.
   * @param id - Unique identifier for the node
   * @param data - Optional data payload associated with the node
   */
  constructor(t, n, i, r = xr(), o = []) {
    E(this, "id");
    E(this, "data");
    E(this, "children");
    E(this, "style");
    E(this, "edgesOut");
    E(this, "edgesIn");
    E(this, "defaultCircleRadius", 10);
    // Layout/physics properties
    E(this, "x");
    E(this, "y");
    E(this, "vx");
    E(this, "vy");
    E(this, "fx");
    E(this, "fy");
    E(this, "weight");
    E(this, "frozen");
    E(this, "visible");
    E(this, "expanded");
    /** True if this node is a child within a collapsed cluster */
    E(this, "isChild");
    E(this, "childrenDepth");
    /** True if this node has child nodes */
    E(this, "isParent");
    /** Reference to the parent cluster node (if this node is a child) */
    E(this, "parentNode");
    /**
     * Reference to the main graph node when this node is a clone in a subgraph.
     * Used for syncing position updates from subgraph back to main graph.
     */
    E(this, "_original_object");
    /**
     * Reference to the deepest sub graph node.
     * Used for checking state of this node in its subgraph
     */
    E(this, "_deepest_node_clone");
    /** The subgraph graph instance created when expanding this node */
    E(this, "_subgraph");
    E(this, "_circleRadius", this.defaultCircleRadius);
    E(this, "_circleRadiusCollapsed", this.defaultCircleRadius);
    /** Measured rectangular border; unset means the node is anchored as a circle. */
    E(this, "_border");
    E(this, "_dirty");
    E(this, "domID");
    this.id = t, this.domID = r, this.data = n ?? {}, this.style = i ?? {}, this.children = [], this.isParent = !1, this.setChildren(o), this._dirty = !0, this.frozen = !1, this.visible = !0, this.expanded = !1, this.isChild = !1, this.childrenDepth = 0, this.edgesOut = /* @__PURE__ */ new Set(), this.edgesIn = /* @__PURE__ */ new Set();
  }
  /**
   * Get the node's data.
   */
  getData() {
    return this.data;
  }
  /**
   * Update the node's data.
   * @param newData - New data to set
   */
  setData(t) {
    this.data = t, this.markDirty();
  }
  /**
   * Merge partial data into the current node data.
   * Useful for updating only parts of the data.
   * @param partialData - Partial data object to merge
   */
  updateData(t) {
    this.data = { ...this.data, ...t }, this.markDirty();
  }
  /**
   * @private
   */
  registerEdgeOut(t) {
    this.edgesOut.add(t);
  }
  /**
   * @private
   */
  registerEdgeIn(t) {
    this.edgesIn.add(t);
  }
  /**
   * @private
   */
  emptyEdges() {
    this.edgesOut.clear(), this.edgesIn.clear();
  }
  getConnectedNodes() {
    return [...this.edgesOut].map((t) => t.to);
  }
  getConnectingNodes() {
    return [...this.edgesIn].map((t) => t.from);
  }
  getEdgesOut() {
    return [...this.edgesOut];
  }
  getEdgesIn() {
    return [...this.edgesIn];
  }
  /**
   * Get the node's data.
   */
  getStyle() {
    return this.style;
  }
  /**
   * Update the node's data.
   * @param newStyle - New data to set
   */
  setStyle(t) {
    this.style = t, this.markDirty();
  }
  /**
   * Merge partial data into the current node data.
   * Useful for updating only parts of the data.
   * @param partialStyle - Partial data object to merge
   */
  updateStyle(t) {
    this.style = { ...this.style, ...t }, this.markDirty();
  }
  getGraphElement() {
    return document ? document.getElementById(`node-${this.domID}`) : null;
  }
  /**
   * Convert node to a simple JSON object representation.
   * @param dataOnly - default: false
   */
  toDict(t = !1) {
    const n = {
      id: this.id,
      data: this.data,
      style: this.style,
      weight: this.weight
      // expanded: this.expanded,
    };
    return t || (n.x = this.x, n.y = this.y, n.vx = this.vx, n.vy = this.vy, n.fx = this.fx, n.fy = this.fy), this.hasChildren() && (n.children = this.children.map((i) => i.toDict(t))), n;
  }
  /**
   * Structured-cloneable payload for the simulation worker (no live parent/children/_subgraph
   * refs, unlike `clone()`). The style is stripped of functions: every resolvable channel may
   * hold one, and no force reads them, so sending them only risks a `DataCloneError`.
   */
  toSimulationDTO() {
    return {
      id: this.id,
      data: this.data,
      style: Sn(this.style),
      weight: this.weight,
      _circleRadius: this._circleRadius,
      x: this.x,
      y: this.y,
      vx: this.vx,
      vy: this.vy,
      fx: this.fx,
      fy: this.fy
    };
  }
  clone() {
    const t = { ...this.data }, n = { ...this.style }, i = new Rr(this.id, t, n);
    return i.x = this.x, i.y = this.y, i.vx = this.vx, i.vy = this.vy, i.fx = this.fx, i.fy = this.fy, i.weight = this.weight, i.frozen = this.frozen, i.visible = this.visible, i.expanded = this.expanded, i.isChild = this.isChild, i.childrenDepth = this.childrenDepth, i.isParent = this.isParent, i.parentNode = this.parentNode, i._circleRadius = this._circleRadius, i.children = this.children.map((r) => r.clone()), i;
  }
  /**
   * @private
   */
  markDirty() {
    this._dirty = !0;
  }
  /**
   * @private
   */
  clearDirty() {
    this._dirty = !1;
  }
  /**
   * @private
   */
  isDirty() {
    return this._dirty;
  }
  freeze() {
    this.frozen = !0, this.fx = this.x, this.fy = this.y;
  }
  unfreeze() {
    this.frozen = !1, this.fx = void 0, this.fy = void 0;
  }
  toggleVisibility(t) {
    t ? this.show() : this.hide(), this.markDirty();
  }
  show() {
    this.visible = !0;
  }
  hide() {
    this.visible = !1;
  }
  toggleExpand(t) {
    t === void 0 ? this.expanded ? this.collapse() : this.expand() : t ? this.expand() : this.collapse(), this.markDirty();
  }
  expand() {
    this.expanded = !0, this._original_object && (this._original_object.expanded = !0);
  }
  collapse() {
    this.expanded = !1, this._original_object && (this._original_object.expanded = !1);
  }
  degree() {
    return this.edgesOut.size + this.edgesIn.size;
  }
  /** The node's clusters, outermost first — empty for a node of the root graph. */
  ancestorChain() {
    const t = [];
    let n = this.parentNode;
    for (; n; )
      t.unshift(n), n = n.parentNode;
    return t;
  }
  /**
   * The node the canvas actually draws for this one: itself when every cluster above it
   * is expanded, otherwise the outermost collapsed cluster — the box hiding it.
   *
   * This answers "which dot on screen stands for this node", not "is it visible": an
   * expanded cluster renders a *separate* subgraph built from `toDict()` data, so a
   * nested node is never drawn by this graph even when its cluster is open.
   */
  canvasRepresentative() {
    for (const t of this.ancestorChain())
      if (!t.expanded) return t;
    return this;
  }
  /**
   * Set the node's circle radius. Also drops any measured rectangular border:
   * the radius is the coarser fact, so every caller that resizes a node keeps
   * anchoring correct by default, and only the drawers that know the rendered
   * shape opt back in through {@link setBorderBox}.
   */
  setCircleRadius(t) {
    this._circleRadius = t, this._border = void 0;
  }
  getCircleRadius() {
    return this._circleRadius;
  }
  setCircleRadiusCollapsed(t) {
    this._circleRadiusCollapsed = t;
  }
  getCircleRadiusCollapsed() {
    return this._circleRadiusCollapsed;
  }
  /**
   * Declare that the node's border is the centred `width`×`height` rectangle it
   * actually renders as, so edges stop on it instead of on the bounding circle.
   * Call it *after* {@link setCircleRadius}, which clears it.
   */
  setBorderBox(t, n) {
    this._border = { halfWidth: t / 2, halfHeight: n / 2 };
  }
  /**
   * The node's rectangular border grown by `outset`, or `undefined` when the
   * node is anchored as a circle.
   */
  getBorderBox(t = 0) {
    if (this._border)
      return { halfWidth: this._border.halfWidth + t, halfHeight: this._border.halfHeight + t };
  }
  /**
   * Distance from the node's centre to its border along the unit direction
   * `(dirX, dirY)`, grown by `outset` — where an edge leaving in that direction
   * should start. Rectangular for a measured node, the circle radius otherwise.
   * Deliberately free of style resolution: this runs for both ends of every
   * edge on every tick.
   */
  getBorderDistance(t, n, i = 0) {
    const r = this._border;
    return r ? ll(r.halfWidth + i, r.halfHeight + i, t, n) : this._circleRadius + i;
  }
  setChildren(t) {
    this.children = t, this.hasChildren() ? this.isParent = !0 : this.isParent = !1;
  }
  hasChildren() {
    return this.children.length > 0;
  }
  markAsChild(t, n) {
    this.isChild = !0, this.childrenDepth = n, this.parentNode = t;
  }
  markAsParent() {
    this.isParent = !0;
  }
  /**
   * Sets the subgraph instance (when opening a cluster).
   * @private
   */
  setSubgraph(t) {
    this._subgraph = t;
  }
  /**
   * Gets the subgraph instance created from this node.
   * Returns undefined if this node didn't created a subgraph.
   * @private
   */
  getSubgraph() {
    return this._subgraph;
  }
  /**
   * Sets a reference to the original node from the main graph.
   * Used when this node is a clone in a subgraph to enable position syncing.
   * @private
   */
  setOriginalObject(t) {
    this._original_object = t;
  }
  /**
   * Gets the reference to the original node from the main graph.
   * Returns undefined if this is not a subgraph clone.
   * @private
   */
  getOriginalObject() {
    return this._original_object;
  }
  /**
   * Sets a reference to the original node from the main graph.
   * Used when this node is a clone in a subgraph to enable position syncing.
   * @private
   */
  setDeepestNodeClone(t) {
    this._deepest_node_clone = t;
  }
  /**
   * Gets the reference to the original node from the main graph.
   * Returns undefined if this is not a subgraph clone.
   * @private
   */
  getDeepestNodeClone() {
    return this._deepest_node_clone;
  }
};
class In {
  /**
   * Create a new Edge instance.
   * @param id - Unique identifier for the edge
   * @param from - Source node
   * @param to - Target node
   * @param data - Optional data payload for the edge
   * @param style - Optional style for the edge
   */
  constructor(t, n, i, r, o, a = null, s) {
    E(this, "id");
    E(this, "from");
    E(this, "to");
    E(this, "directed");
    E(this, "data");
    E(this, "style");
    E(this, "visible");
    /**
     * Whether this edge's layer is switched on. A veto over {@link visible}: every
     * other reason an edge is hidden (endpoints filtered out, a collapsed cluster,
     * a manual hide) is asserted through {@link show} / {@link hide}, and `show`
     * cannot bring an edge back while its layer is off.
     */
    E(this, "layerVisible");
    /**
     * What {@link visible} would be if every layer were on — i.e. visibility from
     * the endpoint, collapse and manual reasons alone. The simulation gates on this
     * rather than on `visible`, so switching a layer off never changes the layout.
     */
    E(this, "visibleIgnoringLayer");
    /**
     * For a cross-cluster stand-in: the real edges it speaks for. Stand-ins are
     * deduped by node *pair*, so one can cover several relations of several kinds;
     * it is filtered out only once every one of them is.
     */
    E(this, "representedEdges");
    /** True if this is a synthetic edge (placeholder for collapsed cluster child) */
    E(this, "isSynthetic");
    /**
     * True for the subclass of synthetic edges that stand in for a real edge whose
     * *both* endpoints are children of different clusters. Unlike the external→cluster
     * synthetic edges, these are resolved as a set (one per collapse state) by
     * {@link ClusterDrawer.resolveCrossClusterEdges} rather than the per-node toggle.
     */
    E(this, "isCrossCluster");
    /** The actual child node this synthetic edge points to (for expansion logic) */
    E(this, "syntheticTerminalNode");
    /** For a cross-cluster synthetic edge: the real child the `from` side stands in for. */
    E(this, "syntheticSourceNode");
    E(this, "_original_object");
    E(this, "_subgraphFromNode");
    E(this, "_subgraphToNode");
    E(this, "_dirty");
    E(this, "domID");
    this.id = t, this.domID = xr(), this.from = n, this.to = i, this.directed = a, this.data = r ?? {}, this.style = o ?? {}, this.visible = !0, this.layerVisible = !0, this.visibleIgnoringLayer = !0, this._dirty = !0, this.isSynthetic = s !== void 0, this.syntheticTerminalNode = s, this.from.registerEdgeOut(this), this.to.registerEdgeIn(this);
  }
  /** Required by d3-force */
  get source() {
    return this.from;
  }
  get target() {
    return this.to;
  }
  /**
   * Get the edge's data.
   */
  getData() {
    return this.data;
  }
  /**
   * Update the edge's data.
   * @param newData - New data to set
   */
  setData(t) {
    this.data = t, this.markDirty();
  }
  /**
   * Merge partial data into the current edge data.
   * @param partialData - Partial data object to merge
   */
  updateData(t) {
    this.data = { ...this.data, ...t }, this.markDirty();
  }
  /**
   * Get the edge's style.
   */
  getStyle() {
    return this.style;
  }
  /**
   * Get the edge's style.
   */
  getEdgeStyle() {
    var t;
    return ((t = this.style) == null ? void 0 : t.edge) ?? {};
  }
  /**
   * Get the edge's label style if available.
   */
  getLabelStyle() {
    var t;
    return ((t = this.style) == null ? void 0 : t.label) ?? {};
  }
  /**
   * Update the edge's style.
   * @param newStyle - New style to set
   */
  setStyle(t) {
    this.style = t, this.markDirty();
  }
  /**
   * Merge partial style into the current edge style.
   * Useful for updating only parts of the style.
   * @param partialStyle - Partial style object to merge
   */
  updateStyle(t) {
    const n = this.style, i = t;
    this.style = {
      ...n,
      ...i,
      edge: { ...n.edge, ...i.edge },
      label: { ...n.label, ...i.label }
    }, this.markDirty();
  }
  getGraphElement() {
    return document ? document.getElementById(`edge-${this.domID}`) : null;
  }
  setFrom(t) {
    this.from = t;
  }
  setTo(t) {
    this.to = t;
  }
  /**
   * Convert edge to a simple JSON object representation.
   */
  toDict() {
    return {
      id: this.id,
      from: this.from.id,
      to: this.to.id,
      data: this.data,
      style: this.style
    };
  }
  /**
   * Structured-cloneable payload for the simulation worker; endpoints reduced to ids, keeps
   * `directed`. The style is stripped of functions — see {@link Node.toSimulationDTO}.
   */
  toSimulationDTO() {
    return {
      id: this.id,
      from: { id: this.from.id },
      to: { id: this.to.id },
      data: this.data,
      style: Sn(this.style),
      directed: this.directed
    };
  }
  clone() {
    const t = { ...this.data }, n = { ...this.style }, i = new In(
      this.id,
      this.from.clone(),
      this.to.clone(),
      t,
      n,
      this.directed
    );
    return i.visible = this.visible, i.layerVisible = this.layerVisible, i.visibleIgnoringLayer = this.visibleIgnoringLayer, i;
  }
  markDirty() {
    this._dirty = !0;
  }
  clearDirty() {
    this._dirty = !1;
  }
  isDirty() {
    return this._dirty;
  }
  toggleVisibility(t) {
    t ? this.show() : this.hide(), this.markDirty();
  }
  show() {
    this.visibleIgnoringLayer = !0, this.visible = this.layerVisible;
  }
  hide() {
    this.visibleIgnoringLayer = !1, this.visible = !1;
  }
  /**
   * Switch this edge's layer on or off, re-deriving {@link visible} from the other
   * reasons it may already be hidden for. Returns whether anything changed.
   */
  setLayerVisible(t) {
    if (this.layerVisible === t) return !1;
    this.layerVisible = t;
    const n = t && this.visibleIgnoringLayer;
    return this.visible !== n && (this.visible = n, this.markDirty()), !0;
  }
  /**
   * Sets a reference to the original node from the main graph.
   * Used when this node is a clone in a subgraph to enable position syncing.
   * @private
   */
  setOriginalObject(t) {
    this._original_object = t;
  }
  /**
   * Gets the reference to the original node from the main graph.
   * Returns undefined if this is not a subgraph clone.
   * @private
   */
  getOriginalObject() {
    return this._original_object;
  }
  /**
   * Sets a reference to the subgraph node from the main graph.
   * Used when the FROM node has a clone in a subgraph
   * @private
   */
  setSubgraphFromNode(t) {
    this._subgraphFromNode = t;
  }
  /**
   * Sets a reference to the subgraph node from the main graph.
   * Used when the TO node has a clone in a subgraph
   * @private
   */
  setSubgraphToNode(t) {
    this._subgraphToNode = t;
  }
  /**
   * Gets the reference to the subgraph node from the main graph.
   * @private
   */
  getSubgraphFromNode() {
    return this._subgraphFromNode;
  }
  /**
   * Gets the reference to the subgraph node from the main graph.
   * @private
   */
  getSubgraphToNode() {
    return this._subgraphToNode;
  }
}
const Mr = 'var Lc=Object.defineProperty;var Fc=(se,kt,pe)=>kt in se?Lc(se,kt,{enumerable:!0,configurable:!0,writable:!0,value:pe}):se[kt]=pe;var E=(se,kt,pe)=>Fc(se,typeof kt!="symbol"?kt+"":kt,pe);(function(){"use strict";function se(e){const t=+this._x.call(null,e),n=+this._y.call(null,e);return kt(this.cover(t,n),t,n,e)}function kt(e,t,n,i){if(isNaN(t)||isNaN(n))return e;var r,o=e._root,a={data:i},s=e._x0,c=e._y0,l=e._x1,f=e._y1,T,d,m,A,b,g,w,_;if(!o)return e._root=a,e;for(;o.length;)if((b=t>=(T=(s+l)/2))?s=T:l=T,(g=n>=(d=(c+f)/2))?c=d:f=d,r=o,!(o=o[w=g<<1|b]))return r[w]=a,e;if(m=+e._x.call(null,o.data),A=+e._y.call(null,o.data),t===m&&n===A)return a.next=o,r?r[w]=a:e._root=a,e;do r=r?r[w]=new Array(4):e._root=new Array(4),(b=t>=(T=(s+l)/2))?s=T:l=T,(g=n>=(d=(c+f)/2))?c=d:f=d;while((w=g<<1|b)===(_=(A>=d)<<1|m>=T));return r[_]=o,r[w]=a,e}function pe(e){var t,n,i=e.length,r,o,a=new Array(i),s=new Array(i),c=1/0,l=1/0,f=-1/0,T=-1/0;for(n=0;n<i;++n)isNaN(r=+this._x.call(null,t=e[n]))||isNaN(o=+this._y.call(null,t))||(a[n]=r,s[n]=o,r<c&&(c=r),r>f&&(f=r),o<l&&(l=o),o>T&&(T=o));if(c>f||l>T)return this;for(this.cover(c,l).cover(f,T),n=0;n<i;++n)kt(this,a[n],s[n],e[n]);return this}function Fr(e,t){if(isNaN(e=+e)||isNaN(t=+t))return this;var n=this._x0,i=this._y0,r=this._x1,o=this._y1;if(isNaN(n))r=(n=Math.floor(e))+1,o=(i=Math.floor(t))+1;else{for(var a=r-n||1,s=this._root,c,l;n>e||e>=r||i>t||t>=o;)switch(l=(t<i)<<1|e<n,c=new Array(4),c[l]=s,s=c,a*=2,l){case 0:r=n+a,o=i+a;break;case 1:n=r-a,o=i+a;break;case 2:r=n+a,i=o-a;break;case 3:n=r-a,i=o-a;break}this._root&&this._root.length&&(this._root=s)}return this._x0=n,this._y0=i,this._x1=r,this._y1=o,this}function kr(){var e=[];return this.visit(function(t){if(!t.length)do e.push(t.data);while(t=t.next)}),e}function Pr(e){return arguments.length?this.cover(+e[0][0],+e[0][1]).cover(+e[1][0],+e[1][1]):isNaN(this._x0)?void 0:[[this._x0,this._y0],[this._x1,this._y1]]}function yt(e,t,n,i,r){this.node=e,this.x0=t,this.y0=n,this.x1=i,this.y1=r}function zr(e,t,n){var i,r=this._x0,o=this._y0,a,s,c,l,f=this._x1,T=this._y1,d=[],m=this._root,A,b;for(m&&d.push(new yt(m,r,o,f,T)),n==null?n=1/0:(r=e-n,o=t-n,f=e+n,T=t+n,n*=n);A=d.pop();)if(!(!(m=A.node)||(a=A.x0)>f||(s=A.y0)>T||(c=A.x1)<r||(l=A.y1)<o))if(m.length){var g=(a+c)/2,w=(s+l)/2;d.push(new yt(m[3],g,w,c,l),new yt(m[2],a,w,g,l),new yt(m[1],g,s,c,w),new yt(m[0],a,s,g,w)),(b=(t>=w)<<1|e>=g)&&(A=d[d.length-1],d[d.length-1]=d[d.length-1-b],d[d.length-1-b]=A)}else{var _=e-+this._x.call(null,m.data),R=t-+this._y.call(null,m.data),y=_*_+R*R;if(y<n){var C=Math.sqrt(n=y);r=e-C,o=t-C,f=e+C,T=t+C,i=m.data}}return i}function Gr(e){if(isNaN(f=+this._x.call(null,e))||isNaN(T=+this._y.call(null,e)))return this;var t,n=this._root,i,r,o,a=this._x0,s=this._y0,c=this._x1,l=this._y1,f,T,d,m,A,b,g,w;if(!n)return this;if(n.length)for(;;){if((A=f>=(d=(a+c)/2))?a=d:c=d,(b=T>=(m=(s+l)/2))?s=m:l=m,t=n,!(n=n[g=b<<1|A]))return this;if(!n.length)break;(t[g+1&3]||t[g+2&3]||t[g+3&3])&&(i=t,w=g)}for(;n.data!==e;)if(r=n,!(n=n.next))return this;return(o=n.next)&&delete n.next,r?(o?r.next=o:delete r.next,this):t?(o?t[g]=o:delete t[g],(n=t[0]||t[1]||t[2]||t[3])&&n===(t[3]||t[2]||t[1]||t[0])&&!n.length&&(i?i[w]=n:this._root=n),this):(this._root=o,this)}function Br(e){for(var t=0,n=e.length;t<n;++t)this.remove(e[t]);return this}function Ur(){return this._root}function Hr(){var e=0;return this.visit(function(t){if(!t.length)do++e;while(t=t.next)}),e}function Wr(e){var t=[],n,i=this._root,r,o,a,s,c;for(i&&t.push(new yt(i,this._x0,this._y0,this._x1,this._y1));n=t.pop();)if(!e(i=n.node,o=n.x0,a=n.y0,s=n.x1,c=n.y1)&&i.length){var l=(o+s)/2,f=(a+c)/2;(r=i[3])&&t.push(new yt(r,l,f,s,c)),(r=i[2])&&t.push(new yt(r,o,f,l,c)),(r=i[1])&&t.push(new yt(r,l,a,s,f)),(r=i[0])&&t.push(new yt(r,o,a,l,f))}return this}function jr(e){var t=[],n=[],i;for(this._root&&t.push(new yt(this._root,this._x0,this._y0,this._x1,this._y1));i=t.pop();){var r=i.node;if(r.length){var o,a=i.x0,s=i.y0,c=i.x1,l=i.y1,f=(a+c)/2,T=(s+l)/2;(o=r[0])&&t.push(new yt(o,a,s,f,T)),(o=r[1])&&t.push(new yt(o,f,s,c,T)),(o=r[2])&&t.push(new yt(o,a,T,f,l)),(o=r[3])&&t.push(new yt(o,f,T,c,l))}n.push(i)}for(;i=n.pop();)e(i.node,i.x0,i.y0,i.x1,i.y1);return this}function Kr(e){return e[0]}function Vr(e){return arguments.length?(this._x=e,this):this._x}function $r(e){return e[1]}function Xr(e){return arguments.length?(this._y=e,this):this._y}function kn(e,t,n){var i=new Pn(t??Kr,n??$r,NaN,NaN,NaN,NaN);return e==null?i:i.addAll(e)}function Pn(e,t,n,i,r,o){this._x=e,this._y=t,this._x0=n,this._y0=i,this._x1=r,this._y1=o,this._root=void 0}function _i(e){for(var t={data:e.data},n=t;e=e.next;)n=n.next={data:e.data};return t}var _t=kn.prototype=Pn.prototype;_t.copy=function(){var e=new Pn(this._x,this._y,this._x0,this._y0,this._x1,this._y1),t=this._root,n,i;if(!t)return e;if(!t.length)return e._root=_i(t),e;for(n=[{source:t,target:e._root=new Array(4)}];t=n.pop();)for(var r=0;r<4;++r)(i=t.source[r])&&(i.length?n.push({source:i,target:t.target[r]=new Array(4)}):t.target[r]=_i(i));return e},_t.add=se,_t.addAll=pe,_t.cover=Fr,_t.data=kr,_t.extent=Pr,_t.find=zr,_t.remove=Gr,_t.removeAll=Br,_t.root=Ur,_t.size=Hr,_t.visit=Wr,_t.visitAfter=jr,_t.x=Vr,_t.y=Xr;function dt(e){return function(){return e}}function Zt(e){return(e()-.5)*1e-6}function Yr(e){return e.x+e.vx}function qr(e){return e.y+e.vy}function Zr(e){var t,n,i,r=1,o=1;typeof e!="function"&&(e=dt(e==null?1:+e));function a(){for(var l,f=t.length,T,d,m,A,b,g,w=0;w<o;++w)for(T=kn(t,Yr,qr).visitAfter(s),l=0;l<f;++l)d=t[l],b=n[d.index],g=b*b,m=d.x+d.vx,A=d.y+d.vy,T.visit(_);function _(R,y,C,O,G){var P=R.data,F=R.r,z=b+F;if(P){if(P.index>d.index){var X=m-P.x-P.vx,nt=A-P.y-P.vy,N=X*X+nt*nt;N<z*z&&(X===0&&(X=Zt(i),N+=X*X),nt===0&&(nt=Zt(i),N+=nt*nt),N=(z-(N=Math.sqrt(N)))/N*r,d.vx+=(X*=N)*(z=(F*=F)/(g+F)),d.vy+=(nt*=N)*z,P.vx-=X*(z=1-z),P.vy-=nt*z)}return}return y>m+z||O<m-z||C>A+z||G<A-z}}function s(l){if(l.data)return l.r=n[l.data.index];for(var f=l.r=0;f<4;++f)l[f]&&l[f].r>l.r&&(l.r=l[f].r)}function c(){if(t){var l,f=t.length,T;for(n=new Array(f),l=0;l<f;++l)T=t[l],n[T.index]=+e(T,l,t)}}return a.initialize=function(l,f){t=l,i=f,c()},a.iterations=function(l){return arguments.length?(o=+l,a):o},a.strength=function(l){return arguments.length?(r=+l,a):r},a.radius=function(l){return arguments.length?(e=typeof l=="function"?l:dt(+l),c(),a):e},a}function Qr(e){return e.index}function Ti(e,t){var n=e.get(t);if(!n)throw new Error("node not found: "+t);return n}function Jr(e){var t=Qr,n=T,i,r=dt(30),o,a,s,c,l,f=1;e==null&&(e=[]);function T(g){return 1/Math.min(s[g.source.index],s[g.target.index])}function d(g){for(var w=0,_=e.length;w<f;++w)for(var R=0,y,C,O,G,P,F,z;R<_;++R)y=e[R],C=y.source,O=y.target,G=O.x+O.vx-C.x-C.vx||Zt(l),P=O.y+O.vy-C.y-C.vy||Zt(l),F=Math.sqrt(G*G+P*P),F=(F-o[R])/F*g*i[R],G*=F,P*=F,O.vx-=G*(z=c[R]),O.vy-=P*z,C.vx+=G*(z=1-z),C.vy+=P*z}function m(){if(a){var g,w=a.length,_=e.length,R=new Map(a.map((C,O)=>[t(C,O,a),C])),y;for(g=0,s=new Array(w);g<_;++g)y=e[g],y.index=g,typeof y.source!="object"&&(y.source=Ti(R,y.source)),typeof y.target!="object"&&(y.target=Ti(R,y.target)),s[y.source.index]=(s[y.source.index]||0)+1,s[y.target.index]=(s[y.target.index]||0)+1;for(g=0,c=new Array(_);g<_;++g)y=e[g],c[g]=s[y.source.index]/(s[y.source.index]+s[y.target.index]);i=new Array(_),A(),o=new Array(_),b()}}function A(){if(a)for(var g=0,w=e.length;g<w;++g)i[g]=+n(e[g],g,e)}function b(){if(a)for(var g=0,w=e.length;g<w;++g)o[g]=+r(e[g],g,e)}return d.initialize=function(g,w){a=g,l=w,m()},d.links=function(g){return arguments.length?(e=g,m(),d):e},d.id=function(g){return arguments.length?(t=g,d):t},d.iterations=function(g){return arguments.length?(f=+g,d):f},d.strength=function(g){return arguments.length?(n=typeof g=="function"?g:dt(+g),A(),d):n},d.distance=function(g){return arguments.length?(r=typeof g=="function"?g:dt(+g),b(),d):r},d}var to={value:()=>{}};function zn(){for(var e=0,t=arguments.length,n={},i;e<t;++e){if(!(i=arguments[e]+"")||i in n||/[\\s.]/.test(i))throw new Error("illegal type: "+i);n[i]=[]}return new Je(n)}function Je(e){this._=e}function eo(e,t){return e.trim().split(/^|\\s+/).map(function(n){var i="",r=n.indexOf(".");if(r>=0&&(i=n.slice(r+1),n=n.slice(0,r)),n&&!t.hasOwnProperty(n))throw new Error("unknown type: "+n);return{type:n,name:i}})}Je.prototype=zn.prototype={constructor:Je,on:function(e,t){var n=this._,i=eo(e+"",n),r,o=-1,a=i.length;if(arguments.length<2){for(;++o<a;)if((r=(e=i[o]).type)&&(r=no(n[r],e.name)))return r;return}if(t!=null&&typeof t!="function")throw new Error("invalid callback: "+t);for(;++o<a;)if(r=(e=i[o]).type)n[r]=bi(n[r],e.name,t);else if(t==null)for(r in n)n[r]=bi(n[r],e.name,null);return this},copy:function(){var e={},t=this._;for(var n in t)e[n]=t[n].slice();return new Je(e)},call:function(e,t){if((r=arguments.length-2)>0)for(var n=new Array(r),i=0,r,o;i<r;++i)n[i]=arguments[i+2];if(!this._.hasOwnProperty(e))throw new Error("unknown type: "+e);for(o=this._[e],i=0,r=o.length;i<r;++i)o[i].value.apply(t,n)},apply:function(e,t,n){if(!this._.hasOwnProperty(e))throw new Error("unknown type: "+e);for(var i=this._[e],r=0,o=i.length;r<o;++r)i[r].value.apply(t,n)}};function no(e,t){for(var n=0,i=e.length,r;n<i;++n)if((r=e[n]).name===t)return r.value}function bi(e,t,n){for(var i=0,r=e.length;i<r;++i)if(e[i].name===t){e[i]=to,e=e.slice(0,i).concat(e.slice(i+1));break}return n!=null&&e.push({name:t,value:n}),e}var ge=0,Ne=0,Re=0,Si=1e3,tn,Ie,en=0,ae=0,nn=0,Me=typeof performance=="object"&&performance.now?performance:Date,wi=typeof window=="object"&&window.requestAnimationFrame?window.requestAnimationFrame.bind(window):function(e){setTimeout(e,17)};function Ai(){return ae||(wi(io),ae=Me.now()+nn)}function io(){ae=0}function Gn(){this._call=this._time=this._next=null}Gn.prototype=vi.prototype={constructor:Gn,restart:function(e,t,n){if(typeof e!="function")throw new TypeError("callback is not a function");n=(n==null?Ai():+n)+(t==null?0:+t),!this._next&&Ie!==this&&(Ie?Ie._next=this:tn=this,Ie=this),this._call=e,this._time=n,Bn()},stop:function(){this._call&&(this._call=null,this._time=1/0,Bn())}};function vi(e,t,n){var i=new Gn;return i.restart(e,t,n),i}function ro(){Ai(),++ge;for(var e=tn,t;e;)(t=ae-e._time)>=0&&e._call.call(void 0,t),e=e._next;--ge}function Ei(){ae=(en=Me.now())+nn,ge=Ne=0;try{ro()}finally{ge=0,so(),ae=0}}function oo(){var e=Me.now(),t=e-en;t>Si&&(nn-=t,en=e)}function so(){for(var e,t=tn,n,i=1/0;t;)t._call?(i>t._time&&(i=t._time),e=t,t=t._next):(n=t._next,t._next=null,t=e?e._next=n:tn=n);Ie=e,Bn(i)}function Bn(e){if(!ge){Ne&&(Ne=clearTimeout(Ne));var t=e-ae;t>24?(e<1/0&&(Ne=setTimeout(Ei,e-Me.now()-nn)),Re&&(Re=clearInterval(Re))):(Re||(en=Me.now(),Re=setInterval(oo,Si)),ge=1,wi(Ei))}}const ao=1664525,lo=1013904223,xi=4294967296;function co(){let e=1;return()=>(e=(ao*e+lo)%xi)/xi}function uo(e){return e.x}function ho(e){return e.y}var fo=10,po=Math.PI*(3-Math.sqrt(5));function go(e){var t,n=1,i=.001,r=1-Math.pow(i,1/300),o=0,a=.6,s=new Map,c=vi(T),l=zn("tick","end"),f=co();e==null&&(e=[]);function T(){d(),l.call("tick",t),n<i&&(c.stop(),l.call("end",t))}function d(b){var g,w=e.length,_;b===void 0&&(b=1);for(var R=0;R<b;++R)for(n+=(o-n)*r,s.forEach(function(y){y(n)}),g=0;g<w;++g)_=e[g],_.fx==null?_.x+=_.vx*=a:(_.x=_.fx,_.vx=0),_.fy==null?_.y+=_.vy*=a:(_.y=_.fy,_.vy=0);return t}function m(){for(var b=0,g=e.length,w;b<g;++b){if(w=e[b],w.index=b,w.fx!=null&&(w.x=w.fx),w.fy!=null&&(w.y=w.fy),isNaN(w.x)||isNaN(w.y)){var _=fo*Math.sqrt(.5+b),R=b*po;w.x=_*Math.cos(R),w.y=_*Math.sin(R)}(isNaN(w.vx)||isNaN(w.vy))&&(w.vx=w.vy=0)}}function A(b){return b.initialize&&b.initialize(e,f),b}return m(),t={tick:d,restart:function(){return c.restart(T),t},stop:function(){return c.stop(),t},nodes:function(b){return arguments.length?(e=b,m(),s.forEach(A),t):e},alpha:function(b){return arguments.length?(n=+b,t):n},alphaMin:function(b){return arguments.length?(i=+b,t):i},alphaDecay:function(b){return arguments.length?(r=+b,t):+r},alphaTarget:function(b){return arguments.length?(o=+b,t):o},velocityDecay:function(b){return arguments.length?(a=1-b,t):1-a},randomSource:function(b){return arguments.length?(f=b,s.forEach(A),t):f},force:function(b,g){return arguments.length>1?(g==null?s.delete(b):s.set(b,A(g)),t):s.get(b)},find:function(b,g,w){var _=0,R=e.length,y,C,O,G,P;for(w==null?w=1/0:w*=w,_=0;_<R;++_)G=e[_],y=b-G.x,C=g-G.y,O=y*y+C*C,O<w&&(P=G,w=O);return P},on:function(b,g){return arguments.length>1?(l.on(b,g),t):l.on(b)}}}function mo(){var e,t,n,i,r=dt(-30),o,a=1,s=1/0,c=.81;function l(m){var A,b=e.length,g=kn(e,uo,ho).visitAfter(T);for(i=m,A=0;A<b;++A)t=e[A],g.visit(d)}function f(){if(e){var m,A=e.length,b;for(o=new Array(A),m=0;m<A;++m)b=e[m],o[b.index]=+r(b,m,e)}}function T(m){var A=0,b,g,w=0,_,R,y;if(m.length){for(_=R=y=0;y<4;++y)(b=m[y])&&(g=Math.abs(b.value))&&(A+=b.value,w+=g,_+=g*b.x,R+=g*b.y);m.x=_/w,m.y=R/w}else{b=m,b.x=b.data.x,b.y=b.data.y;do A+=o[b.data.index];while(b=b.next)}m.value=A}function d(m,A,b,g){if(!m.value)return!0;var w=m.x-t.x,_=m.y-t.y,R=g-A,y=w*w+_*_;if(R*R/c<y)return y<s&&(w===0&&(w=Zt(n),y+=w*w),_===0&&(_=Zt(n),y+=_*_),y<a&&(y=Math.sqrt(a*y)),t.vx+=w*m.value*i/y,t.vy+=_*m.value*i/y),!0;if(m.length||y>=s)return;(m.data!==t||m.next)&&(w===0&&(w=Zt(n),y+=w*w),_===0&&(_=Zt(n),y+=_*_),y<a&&(y=Math.sqrt(a*y)));do m.data!==t&&(R=o[m.data.index]*i/y,t.vx+=w*R,t.vy+=_*R);while(m=m.next)}return l.initialize=function(m,A){e=m,n=A,f()},l.strength=function(m){return arguments.length?(r=typeof m=="function"?m:dt(+m),f(),l):r},l.distanceMin=function(m){return arguments.length?(a=m*m,l):Math.sqrt(a)},l.distanceMax=function(m){return arguments.length?(s=m*m,l):Math.sqrt(s)},l.theta=function(m){return arguments.length?(c=m*m,l):Math.sqrt(c)},l}function Ni(e,t,n){var i,r=dt(.1),o,a;typeof e!="function"&&(e=dt(+e)),t==null&&(t=0),n==null&&(n=0);function s(l){for(var f=0,T=i.length;f<T;++f){var d=i[f],m=d.x-t||1e-6,A=d.y-n||1e-6,b=Math.sqrt(m*m+A*A),g=(a[f]-b)*o[f]*l/b;d.vx+=m*g,d.vy+=A*g}}function c(){if(i){var l,f=i.length;for(o=new Array(f),a=new Array(f),l=0;l<f;++l)a[l]=+e(i[l],l,i),o[l]=isNaN(a[l])?0:+r(i[l],l,i)}}return s.initialize=function(l){i=l,c()},s.strength=function(l){return arguments.length?(r=typeof l=="function"?l:dt(+l),c(),s):r},s.radius=function(l){return arguments.length?(e=typeof l=="function"?l:dt(+l),c(),s):e},s.x=function(l){return arguments.length?(t=+l,s):t},s.y=function(l){return arguments.length?(n=+l,s):n},s}function Ri(e){var t=dt(.1),n,i,r;typeof e!="function"&&(e=dt(e==null?0:+e));function o(s){for(var c=0,l=n.length,f;c<l;++c)f=n[c],f.vx+=(r[c]-f.x)*i[c]*s}function a(){if(n){var s,c=n.length;for(i=new Array(c),r=new Array(c),s=0;s<c;++s)i[s]=isNaN(r[s]=+e(n[s],s,n))?0:+t(n[s],s,n)}}return o.initialize=function(s){n=s,a()},o.strength=function(s){return arguments.length?(t=typeof s=="function"?s:dt(+s),a(),o):t},o.x=function(s){return arguments.length?(e=typeof s=="function"?s:dt(+s),a(),o):e},o}function Ii(e){var t=dt(.1),n,i,r;typeof e!="function"&&(e=dt(e==null?0:+e));function o(s){for(var c=0,l=n.length,f;c<l;++c)f=n[c],f.vy+=(r[c]-f.y)*i[c]*s}function a(){if(n){var s,c=n.length;for(i=new Array(c),r=new Array(c),s=0;s<c;++s)i[s]=isNaN(r[s]=+e(n[s],s,n))?0:+t(n[s],s,n)}}return o.initialize=function(s){n=s,a()},o.strength=function(s){return arguments.length?(t=typeof s=="function"?s:dt(+s),a(),o):t},o.y=function(s){return arguments.length?(e=typeof s=="function"?s:dt(+s),a(),o):e},o}function yo(e=0,t=0,n=.001){let i=[],r;function o(){r=typeof n=="function"?n:()=>n}function a(s){for(let c=0,l=i.length;c<l;++c){const f=i[c],T=r(f,c,i);f.vx!=null&&f.x!=null&&(f.vx-=(f.x-e)*T*s),f.vy!=null&&f.y!=null&&(f.vy-=(f.y-t)*T*s)}}return a.initialize=s=>{i=s,o()},a.x=function(s){return arguments.length?(e=s,a):e},a.y=function(s){return arguments.length?(t=s,a):t},a.strength=function(s){return arguments.length?(n=s,o(),a):n},a}var Un="http://www.w3.org/1999/xhtml",Mi={svg:"http://www.w3.org/2000/svg",xhtml:Un,xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/"};function Ci(e){var t=e+="",n=t.indexOf(":");return n>=0&&(t=e.slice(0,n))!=="xmlns"&&(e=e.slice(n+1)),Mi.hasOwnProperty(t)?{space:Mi[t],local:e}:e}function _o(e){return function(){var t=this.ownerDocument,n=this.namespaceURI;return n===Un&&t.documentElement.namespaceURI===Un?t.createElement(e):t.createElementNS(n,e)}}function To(e){return function(){return this.ownerDocument.createElementNS(e.space,e.local)}}function Di(e){var t=Ci(e);return(t.local?To:_o)(t)}function bo(){}function Oi(e){return e==null?bo:function(){return this.querySelector(e)}}function So(e){typeof e!="function"&&(e=Oi(e));for(var t=this._groups,n=t.length,i=new Array(n),r=0;r<n;++r)for(var o=t[r],a=o.length,s=i[r]=new Array(a),c,l,f=0;f<a;++f)(c=o[f])&&(l=e.call(c,c.__data__,f,o))&&("__data__"in c&&(l.__data__=c.__data__),s[f]=l);return new It(i,this._parents)}function wo(e){return e==null?[]:Array.isArray(e)?e:Array.from(e)}function Ao(){return[]}function vo(e){return e==null?Ao:function(){return this.querySelectorAll(e)}}function Eo(e){return function(){return wo(e.apply(this,arguments))}}function xo(e){typeof e=="function"?e=Eo(e):e=vo(e);for(var t=this._groups,n=t.length,i=[],r=[],o=0;o<n;++o)for(var a=t[o],s=a.length,c,l=0;l<s;++l)(c=a[l])&&(i.push(e.call(c,c.__data__,l,a)),r.push(c));return new It(i,r)}function No(e){return function(){return this.matches(e)}}function Li(e){return function(t){return t.matches(e)}}var Ro=Array.prototype.find;function Io(e){return function(){return Ro.call(this.children,e)}}function Mo(){return this.firstElementChild}function Co(e){return this.select(e==null?Mo:Io(typeof e=="function"?e:Li(e)))}var Do=Array.prototype.filter;function Oo(){return Array.from(this.children)}function Lo(e){return function(){return Do.call(this.children,e)}}function Fo(e){return this.selectAll(e==null?Oo:Lo(typeof e=="function"?e:Li(e)))}function ko(e){typeof e!="function"&&(e=No(e));for(var t=this._groups,n=t.length,i=new Array(n),r=0;r<n;++r)for(var o=t[r],a=o.length,s=i[r]=[],c,l=0;l<a;++l)(c=o[l])&&e.call(c,c.__data__,l,o)&&s.push(c);return new It(i,this._parents)}function Fi(e){return new Array(e.length)}function Po(){return new It(this._enter||this._groups.map(Fi),this._parents)}function rn(e,t){this.ownerDocument=e.ownerDocument,this.namespaceURI=e.namespaceURI,this._next=null,this._parent=e,this.__data__=t}rn.prototype={constructor:rn,appendChild:function(e){return this._parent.insertBefore(e,this._next)},insertBefore:function(e,t){return this._parent.insertBefore(e,t)},querySelector:function(e){return this._parent.querySelector(e)},querySelectorAll:function(e){return this._parent.querySelectorAll(e)}};function zo(e){return function(){return e}}function Go(e,t,n,i,r,o){for(var a=0,s,c=t.length,l=o.length;a<l;++a)(s=t[a])?(s.__data__=o[a],i[a]=s):n[a]=new rn(e,o[a]);for(;a<c;++a)(s=t[a])&&(r[a]=s)}function Bo(e,t,n,i,r,o,a){var s,c,l=new Map,f=t.length,T=o.length,d=new Array(f),m;for(s=0;s<f;++s)(c=t[s])&&(d[s]=m=a.call(c,c.__data__,s,t)+"",l.has(m)?r[s]=c:l.set(m,c));for(s=0;s<T;++s)m=a.call(e,o[s],s,o)+"",(c=l.get(m))?(i[s]=c,c.__data__=o[s],l.delete(m)):n[s]=new rn(e,o[s]);for(s=0;s<f;++s)(c=t[s])&&l.get(d[s])===c&&(r[s]=c)}function Uo(e){return e.__data__}function Ho(e,t){if(!arguments.length)return Array.from(this,Uo);var n=t?Bo:Go,i=this._parents,r=this._groups;typeof e!="function"&&(e=zo(e));for(var o=r.length,a=new Array(o),s=new Array(o),c=new Array(o),l=0;l<o;++l){var f=i[l],T=r[l],d=T.length,m=Wo(e.call(f,f&&f.__data__,l,i)),A=m.length,b=s[l]=new Array(A),g=a[l]=new Array(A),w=c[l]=new Array(d);n(f,T,b,g,w,m,t);for(var _=0,R=0,y,C;_<A;++_)if(y=b[_]){for(_>=R&&(R=_+1);!(C=g[R])&&++R<A;);y._next=C||null}}return a=new It(a,i),a._enter=s,a._exit=c,a}function Wo(e){return typeof e=="object"&&"length"in e?e:Array.from(e)}function jo(){return new It(this._exit||this._groups.map(Fi),this._parents)}function Ko(e,t,n){var i=this.enter(),r=this,o=this.exit();return typeof e=="function"?(i=e(i),i&&(i=i.selection())):i=i.append(e+""),t!=null&&(r=t(r),r&&(r=r.selection())),n==null?o.remove():n(o),i&&r?i.merge(r).order():r}function Vo(e){for(var t=e.selection?e.selection():e,n=this._groups,i=t._groups,r=n.length,o=i.length,a=Math.min(r,o),s=new Array(r),c=0;c<a;++c)for(var l=n[c],f=i[c],T=l.length,d=s[c]=new Array(T),m,A=0;A<T;++A)(m=l[A]||f[A])&&(d[A]=m);for(;c<r;++c)s[c]=n[c];return new It(s,this._parents)}function $o(){for(var e=this._groups,t=-1,n=e.length;++t<n;)for(var i=e[t],r=i.length-1,o=i[r],a;--r>=0;)(a=i[r])&&(o&&a.compareDocumentPosition(o)^4&&o.parentNode.insertBefore(a,o),o=a);return this}function Xo(e){e||(e=Yo);function t(T,d){return T&&d?e(T.__data__,d.__data__):!T-!d}for(var n=this._groups,i=n.length,r=new Array(i),o=0;o<i;++o){for(var a=n[o],s=a.length,c=r[o]=new Array(s),l,f=0;f<s;++f)(l=a[f])&&(c[f]=l);c.sort(t)}return new It(r,this._parents).order()}function Yo(e,t){return e<t?-1:e>t?1:e>=t?0:NaN}function qo(){var e=arguments[0];return arguments[0]=this,e.apply(null,arguments),this}function Zo(){return Array.from(this)}function Qo(){for(var e=this._groups,t=0,n=e.length;t<n;++t)for(var i=e[t],r=0,o=i.length;r<o;++r){var a=i[r];if(a)return a}return null}function Jo(){let e=0;for(const t of this)++e;return e}function ts(){return!this.node()}function es(e){for(var t=this._groups,n=0,i=t.length;n<i;++n)for(var r=t[n],o=0,a=r.length,s;o<a;++o)(s=r[o])&&e.call(s,s.__data__,o,r);return this}function ns(e){return function(){this.removeAttribute(e)}}function is(e){return function(){this.removeAttributeNS(e.space,e.local)}}function rs(e,t){return function(){this.setAttribute(e,t)}}function os(e,t){return function(){this.setAttributeNS(e.space,e.local,t)}}function ss(e,t){return function(){var n=t.apply(this,arguments);n==null?this.removeAttribute(e):this.setAttribute(e,n)}}function as(e,t){return function(){var n=t.apply(this,arguments);n==null?this.removeAttributeNS(e.space,e.local):this.setAttributeNS(e.space,e.local,n)}}function ls(e,t){var n=Ci(e);if(arguments.length<2){var i=this.node();return n.local?i.getAttributeNS(n.space,n.local):i.getAttribute(n)}return this.each((t==null?n.local?is:ns:typeof t=="function"?n.local?as:ss:n.local?os:rs)(n,t))}function ki(e){return e.ownerDocument&&e.ownerDocument.defaultView||e.document&&e||e.defaultView}function cs(e){return function(){this.style.removeProperty(e)}}function us(e,t,n){return function(){this.style.setProperty(e,t,n)}}function hs(e,t,n){return function(){var i=t.apply(this,arguments);i==null?this.style.removeProperty(e):this.style.setProperty(e,i,n)}}function fs(e,t,n){return arguments.length>1?this.each((t==null?cs:typeof t=="function"?hs:us)(e,t,n??"")):ds(this.node(),e)}function ds(e,t){return e.style.getPropertyValue(t)||ki(e).getComputedStyle(e,null).getPropertyValue(t)}function ps(e){return function(){delete this[e]}}function gs(e,t){return function(){this[e]=t}}function ms(e,t){return function(){var n=t.apply(this,arguments);n==null?delete this[e]:this[e]=n}}function ys(e,t){return arguments.length>1?this.each((t==null?ps:typeof t=="function"?ms:gs)(e,t)):this.node()[e]}function Pi(e){return e.trim().split(/^|\\s+/)}function Hn(e){return e.classList||new zi(e)}function zi(e){this._node=e,this._names=Pi(e.getAttribute("class")||"")}zi.prototype={add:function(e){var t=this._names.indexOf(e);t<0&&(this._names.push(e),this._node.setAttribute("class",this._names.join(" ")))},remove:function(e){var t=this._names.indexOf(e);t>=0&&(this._names.splice(t,1),this._node.setAttribute("class",this._names.join(" ")))},contains:function(e){return this._names.indexOf(e)>=0}};function Gi(e,t){for(var n=Hn(e),i=-1,r=t.length;++i<r;)n.add(t[i])}function Bi(e,t){for(var n=Hn(e),i=-1,r=t.length;++i<r;)n.remove(t[i])}function _s(e){return function(){Gi(this,e)}}function Ts(e){return function(){Bi(this,e)}}function bs(e,t){return function(){(t.apply(this,arguments)?Gi:Bi)(this,e)}}function Ss(e,t){var n=Pi(e+"");if(arguments.length<2){for(var i=Hn(this.node()),r=-1,o=n.length;++r<o;)if(!i.contains(n[r]))return!1;return!0}return this.each((typeof t=="function"?bs:t?_s:Ts)(n,t))}function ws(){this.textContent=""}function As(e){return function(){this.textContent=e}}function vs(e){return function(){var t=e.apply(this,arguments);this.textContent=t??""}}function Es(e){return arguments.length?this.each(e==null?ws:(typeof e=="function"?vs:As)(e)):this.node().textContent}function xs(){this.innerHTML=""}function Ns(e){return function(){this.innerHTML=e}}function Rs(e){return function(){var t=e.apply(this,arguments);this.innerHTML=t??""}}function Is(e){return arguments.length?this.each(e==null?xs:(typeof e=="function"?Rs:Ns)(e)):this.node().innerHTML}function Ms(){this.nextSibling&&this.parentNode.appendChild(this)}function Cs(){return this.each(Ms)}function Ds(){this.previousSibling&&this.parentNode.insertBefore(this,this.parentNode.firstChild)}function Os(){return this.each(Ds)}function Ls(e){var t=typeof e=="function"?e:Di(e);return this.select(function(){return this.appendChild(t.apply(this,arguments))})}function Fs(){return null}function ks(e,t){var n=typeof e=="function"?e:Di(e),i=t==null?Fs:typeof t=="function"?t:Oi(t);return this.select(function(){return this.insertBefore(n.apply(this,arguments),i.apply(this,arguments)||null)})}function Ps(){var e=this.parentNode;e&&e.removeChild(this)}function zs(){return this.each(Ps)}function Gs(){var e=this.cloneNode(!1),t=this.parentNode;return t?t.insertBefore(e,this.nextSibling):e}function Bs(){var e=this.cloneNode(!0),t=this.parentNode;return t?t.insertBefore(e,this.nextSibling):e}function Us(e){return this.select(e?Bs:Gs)}function Hs(e){return arguments.length?this.property("__data__",e):this.node().__data__}function Ws(e){return function(t){e.call(this,t,this.__data__)}}function js(e){return e.trim().split(/^|\\s+/).map(function(t){var n="",i=t.indexOf(".");return i>=0&&(n=t.slice(i+1),t=t.slice(0,i)),{type:t,name:n}})}function Ks(e){return function(){var t=this.__on;if(t){for(var n=0,i=-1,r=t.length,o;n<r;++n)o=t[n],(!e.type||o.type===e.type)&&o.name===e.name?this.removeEventListener(o.type,o.listener,o.options):t[++i]=o;++i?t.length=i:delete this.__on}}}function Vs(e,t,n){return function(){var i=this.__on,r,o=Ws(t);if(i){for(var a=0,s=i.length;a<s;++a)if((r=i[a]).type===e.type&&r.name===e.name){this.removeEventListener(r.type,r.listener,r.options),this.addEventListener(r.type,r.listener=o,r.options=n),r.value=t;return}}this.addEventListener(e.type,o,n),r={type:e.type,name:e.name,value:t,listener:o,options:n},i?i.push(r):this.__on=[r]}}function $s(e,t,n){var i=js(e+""),r,o=i.length,a;if(arguments.length<2){var s=this.node().__on;if(s){for(var c=0,l=s.length,f;c<l;++c)for(r=0,f=s[c];r<o;++r)if((a=i[r]).type===f.type&&a.name===f.name)return f.value}return}for(s=t?Vs:Ks,r=0;r<o;++r)this.each(s(i[r],t,n));return this}function Ui(e,t,n){var i=ki(e),r=i.CustomEvent;typeof r=="function"?r=new r(t,n):(r=i.document.createEvent("Event"),n?(r.initEvent(t,n.bubbles,n.cancelable),r.detail=n.detail):r.initEvent(t,!1,!1)),e.dispatchEvent(r)}function Xs(e,t){return function(){return Ui(this,e,t)}}function Ys(e,t){return function(){return Ui(this,e,t.apply(this,arguments))}}function qs(e,t){return this.each((typeof t=="function"?Ys:Xs)(e,t))}function*Zs(){for(var e=this._groups,t=0,n=e.length;t<n;++t)for(var i=e[t],r=0,o=i.length,a;r<o;++r)(a=i[r])&&(yield a)}var Qs=[null];function It(e,t){this._groups=e,this._parents=t}function Js(){return this}It.prototype={constructor:It,select:So,selectAll:xo,selectChild:Co,selectChildren:Fo,filter:ko,data:Ho,enter:Po,exit:jo,join:Ko,merge:Vo,selection:Js,order:$o,sort:Xo,call:qo,nodes:Zo,node:Qo,size:Jo,empty:ts,each:es,attr:ls,style:fs,property:ys,classed:Ss,text:Es,html:Is,raise:Cs,lower:Os,append:Ls,insert:ks,remove:zs,clone:Us,datum:Hs,on:$s,dispatch:qs,[Symbol.iterator]:Zs};function on(e){return typeof e=="string"?new It([[document.querySelector(e)]],[document.documentElement]):new It([[e]],Qs)}function ta(e){let t;for(;t=e.sourceEvent;)e=t;return e}function Hi(e,t){if(e=ta(e),t===void 0&&(t=e.currentTarget),t){var n=t.ownerSVGElement||t;if(n.createSVGPoint){var i=n.createSVGPoint();return i.x=e.clientX,i.y=e.clientY,i=i.matrixTransform(t.getScreenCTM().inverse()),[i.x,i.y]}if(t.getBoundingClientRect){var r=t.getBoundingClientRect();return[e.clientX-r.left-t.clientLeft,e.clientY-r.top-t.clientTop]}}return[e.pageX,e.pageY]}const ea={passive:!1},Ce={capture:!0,passive:!1};function Wn(e){e.stopImmediatePropagation()}function me(e){e.preventDefault(),e.stopImmediatePropagation()}function na(e){var t=e.document.documentElement,n=on(e).on("dragstart.drag",me,Ce);"onselectstart"in t?n.on("selectstart.drag",me,Ce):(t.__noselect=t.style.MozUserSelect,t.style.MozUserSelect="none")}function ia(e,t){var n=e.document.documentElement,i=on(e).on("dragstart.drag",null);t&&(i.on("click.drag",me,Ce),setTimeout(function(){i.on("click.drag",null)},0)),"onselectstart"in n?i.on("selectstart.drag",null):(n.style.MozUserSelect=n.__noselect,delete n.__noselect)}var sn=e=>()=>e;function jn(e,{sourceEvent:t,subject:n,target:i,identifier:r,active:o,x:a,y:s,dx:c,dy:l,dispatch:f}){Object.defineProperties(this,{type:{value:e,enumerable:!0,configurable:!0},sourceEvent:{value:t,enumerable:!0,configurable:!0},subject:{value:n,enumerable:!0,configurable:!0},target:{value:i,enumerable:!0,configurable:!0},identifier:{value:r,enumerable:!0,configurable:!0},active:{value:o,enumerable:!0,configurable:!0},x:{value:a,enumerable:!0,configurable:!0},y:{value:s,enumerable:!0,configurable:!0},dx:{value:c,enumerable:!0,configurable:!0},dy:{value:l,enumerable:!0,configurable:!0},_:{value:f}})}jn.prototype.on=function(){var e=this._.on.apply(this._,arguments);return e===this._?this:e};function ra(e){return!e.ctrlKey&&!e.button}function oa(){return this.parentNode}function sa(e,t){return t??{x:e.x,y:e.y}}function aa(){return navigator.maxTouchPoints||"ontouchstart"in this}function la(){var e=ra,t=oa,n=sa,i=aa,r={},o=zn("start","drag","end"),a=0,s,c,l,f,T=0;function d(y){y.on("mousedown.drag",m).filter(i).on("touchstart.drag",g).on("touchmove.drag",w,ea).on("touchend.drag touchcancel.drag",_).style("touch-action","none").style("-webkit-tap-highlight-color","rgba(0,0,0,0)")}function m(y,C){if(!(f||!e.call(this,y,C))){var O=R(this,t.call(this,y,C),y,C,"mouse");O&&(on(y.view).on("mousemove.drag",A,Ce).on("mouseup.drag",b,Ce),na(y.view),Wn(y),l=!1,s=y.clientX,c=y.clientY,O("start",y))}}function A(y){if(me(y),!l){var C=y.clientX-s,O=y.clientY-c;l=C*C+O*O>T}r.mouse("drag",y)}function b(y){on(y.view).on("mousemove.drag mouseup.drag",null),ia(y.view,l),me(y),r.mouse("end",y)}function g(y,C){if(e.call(this,y,C)){var O=y.changedTouches,G=t.call(this,y,C),P=O.length,F,z;for(F=0;F<P;++F)(z=R(this,G,y,C,O[F].identifier,O[F]))&&(Wn(y),z("start",y,O[F]))}}function w(y){var C=y.changedTouches,O=C.length,G,P;for(G=0;G<O;++G)(P=r[C[G].identifier])&&(me(y),P("drag",y,C[G]))}function _(y){var C=y.changedTouches,O=C.length,G,P;for(f&&clearTimeout(f),f=setTimeout(function(){f=null},500),G=0;G<O;++G)(P=r[C[G].identifier])&&(Wn(y),P("end",y,C[G]))}function R(y,C,O,G,P,F){var z=o.copy(),X=Hi(F||O,C),nt,N,D;if((D=n.call(y,new jn("beforestart",{sourceEvent:O,target:d,identifier:P,active:a,x:X[0],y:X[1],dx:0,dy:0,dispatch:z}),G))!=null)return nt=D.x-X[0]||0,N=D.y-X[1]||0,function K(j,I,B){var Z=X,ut;switch(j){case"start":r[P]=K,ut=a++;break;case"end":delete r[P],--a;case"drag":X=Hi(B||I,C),ut=a;break}z.call(j,y,new jn(j,{sourceEvent:I,subject:D,target:d,identifier:P,active:ut,x:X[0]+nt,y:X[1]+N,dx:X[0]-Z[0],dy:X[1]-Z[1],dispatch:z}),G)}}return d.filter=function(y){return arguments.length?(e=typeof y=="function"?y:sn(!!y),d):e},d.container=function(y){return arguments.length?(t=typeof y=="function"?y:sn(y),d):t},d.subject=function(y){return arguments.length?(n=typeof y=="function"?y:sn(y),d):n},d.touchable=function(y){return arguments.length?(i=typeof y=="function"?y:sn(!!y),d):i},d.on=function(){var y=o.on.apply(o,arguments);return y===o?d:y},d.clickDistance=function(y){return arguments.length?(T=(y=+y)*y,d):Math.sqrt(T)},d}/*! @license DOMPurify 3.4.7 | (c) Cure53 and other contributors | Released under the Apache license 2.0 and Mozilla Public License 2.0 | github.com/cure53/DOMPurify/blob/3.4.7/LICENSE */function Wi(e,t){(t==null||t>e.length)&&(t=e.length);for(var n=0,i=Array(t);n<t;n++)i[n]=e[n];return i}function ca(e){if(Array.isArray(e))return e}function ua(e,t){var n=e==null?null:typeof Symbol<"u"&&e[Symbol.iterator]||e["@@iterator"];if(n!=null){var i,r,o,a,s=[],c=!0,l=!1;try{if(o=(n=n.call(e)).next,t!==0)for(;!(c=(i=o.call(n)).done)&&(s.push(i.value),s.length!==t);c=!0);}catch(f){l=!0,r=f}finally{try{if(!c&&n.return!=null&&(a=n.return(),Object(a)!==a))return}finally{if(l)throw r}}return s}}function ha(){throw new TypeError(`Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method.`)}function fa(e,t){return ca(e)||ua(e,t)||da(e,t)||ha()}function da(e,t){if(e){if(typeof e=="string")return Wi(e,t);var n={}.toString.call(e).slice(8,-1);return n==="Object"&&e.constructor&&(n=e.constructor.name),n==="Map"||n==="Set"?Array.from(e):n==="Arguments"||/^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)?Wi(e,t):void 0}}const ji=Object.entries,Ki=Object.setPrototypeOf,pa=Object.isFrozen,ga=Object.getPrototypeOf,ma=Object.getOwnPropertyDescriptor;let Tt=Object.freeze,Mt=Object.seal,ye=Object.create,Vi=typeof Reflect<"u"&&Reflect,Kn=Vi.apply,Vn=Vi.construct;Tt||(Tt=function(t){return t}),Mt||(Mt=function(t){return t}),Kn||(Kn=function(t,n){for(var i=arguments.length,r=new Array(i>2?i-2:0),o=2;o<i;o++)r[o-2]=arguments[o];return t.apply(n,r)}),Vn||(Vn=function(t){for(var n=arguments.length,i=new Array(n>1?n-1:0),r=1;r<n;r++)i[r-1]=arguments[r];return new t(...i)});const _e=ht(Array.prototype.forEach),ya=ht(Array.prototype.lastIndexOf),$i=ht(Array.prototype.pop),Te=ht(Array.prototype.push),_a=ht(Array.prototype.splice),bt=Array.isArray,De=ht(String.prototype.toLowerCase),$n=ht(String.prototype.toString),Xi=ht(String.prototype.match),be=ht(String.prototype.replace),Yi=ht(String.prototype.indexOf),Ta=ht(String.prototype.trim),ba=ht(Number.prototype.toString),Sa=ht(Boolean.prototype.toString),qi=typeof BigInt>"u"?null:ht(BigInt.prototype.toString),Zi=typeof Symbol>"u"?null:ht(Symbol.prototype.toString),ct=ht(Object.prototype.hasOwnProperty),Oe=ht(Object.prototype.toString),pt=ht(RegExp.prototype.test),Le=wa(TypeError);function ht(e){return function(t){t instanceof RegExp&&(t.lastIndex=0);for(var n=arguments.length,i=new Array(n>1?n-1:0),r=1;r<n;r++)i[r-1]=arguments[r];return Kn(e,t,i)}}function wa(e){return function(){for(var t=arguments.length,n=new Array(t),i=0;i<t;i++)n[i]=arguments[i];return Vn(e,n)}}function W(e,t){let n=arguments.length>2&&arguments[2]!==void 0?arguments[2]:De;if(Ki&&Ki(e,null),!bt(t))return e;let i=t.length;for(;i--;){let r=t[i];if(typeof r=="string"){const o=n(r);o!==r&&(pa(t)||(t[i]=o),r=o)}e[r]=!0}return e}function Aa(e){for(let t=0;t<e.length;t++)ct(e,t)||(e[t]=null);return e}function gt(e){const t=ye(null);for(const i of ji(e)){var n=fa(i,2);const r=n[0],o=n[1];ct(e,r)&&(bt(o)?t[r]=Aa(o):o&&typeof o=="object"&&o.constructor===Object?t[r]=gt(o):t[r]=o)}return t}function va(e){switch(typeof e){case"string":return e;case"number":return ba(e);case"boolean":return Sa(e);case"bigint":return qi?qi(e):"0";case"symbol":return Zi?Zi(e):"Symbol()";case"undefined":return Oe(e);case"function":case"object":{if(e===null)return Oe(e);const t=e,n=Pt(t,"toString");if(typeof n=="function"){const i=n(t);return typeof i=="string"?i:Oe(i)}return Oe(e)}default:return Oe(e)}}function Pt(e,t){for(;e!==null;){const i=ma(e,t);if(i){if(i.get)return ht(i.get);if(typeof i.value=="function")return ht(i.value)}e=ga(e)}function n(){return null}return n}function Ea(e){try{return pt(e,""),!0}catch{return!1}}const Qi=Tt(["a","abbr","acronym","address","area","article","aside","audio","b","bdi","bdo","big","blink","blockquote","body","br","button","canvas","caption","center","cite","code","col","colgroup","content","data","datalist","dd","decorator","del","details","dfn","dialog","dir","div","dl","dt","element","em","fieldset","figcaption","figure","font","footer","form","h1","h2","h3","h4","h5","h6","head","header","hgroup","hr","html","i","img","input","ins","kbd","label","legend","li","main","map","mark","marquee","menu","menuitem","meter","nav","nobr","ol","optgroup","option","output","p","picture","pre","progress","q","rp","rt","ruby","s","samp","search","section","select","shadow","slot","small","source","spacer","span","strike","strong","style","sub","summary","sup","table","tbody","td","template","textarea","tfoot","th","thead","time","tr","track","tt","u","ul","var","video","wbr"]),Xn=Tt(["svg","a","altglyph","altglyphdef","altglyphitem","animatecolor","animatemotion","animatetransform","circle","clippath","defs","desc","ellipse","enterkeyhint","exportparts","filter","font","g","glyph","glyphref","hkern","image","inputmode","line","lineargradient","marker","mask","metadata","mpath","part","path","pattern","polygon","polyline","radialgradient","rect","stop","style","switch","symbol","text","textpath","title","tref","tspan","view","vkern"]),Yn=Tt(["feBlend","feColorMatrix","feComponentTransfer","feComposite","feConvolveMatrix","feDiffuseLighting","feDisplacementMap","feDistantLight","feDropShadow","feFlood","feFuncA","feFuncB","feFuncG","feFuncR","feGaussianBlur","feImage","feMerge","feMergeNode","feMorphology","feOffset","fePointLight","feSpecularLighting","feSpotLight","feTile","feTurbulence"]),xa=Tt(["animate","color-profile","cursor","discard","font-face","font-face-format","font-face-name","font-face-src","font-face-uri","foreignobject","hatch","hatchpath","mesh","meshgradient","meshpatch","meshrow","missing-glyph","script","set","solidcolor","unknown","use"]),qn=Tt(["math","menclose","merror","mfenced","mfrac","mglyph","mi","mlabeledtr","mmultiscripts","mn","mo","mover","mpadded","mphantom","mroot","mrow","ms","mspace","msqrt","mstyle","msub","msup","msubsup","mtable","mtd","mtext","mtr","munder","munderover","mprescripts"]),Na=Tt(["maction","maligngroup","malignmark","mlongdiv","mscarries","mscarry","msgroup","mstack","msline","msrow","semantics","annotation","annotation-xml","mprescripts","none"]),Ji=Tt(["#text"]),tr=Tt(["accept","action","align","alt","autocapitalize","autocomplete","autopictureinpicture","autoplay","background","bgcolor","border","capture","cellpadding","cellspacing","checked","cite","class","clear","color","cols","colspan","command","commandfor","controls","controlslist","coords","crossorigin","datetime","decoding","default","dir","disabled","disablepictureinpicture","disableremoteplayback","download","draggable","enctype","enterkeyhint","exportparts","face","for","headers","height","hidden","high","href","hreflang","id","inert","inputmode","integrity","ismap","kind","label","lang","list","loading","loop","low","max","maxlength","media","method","min","minlength","multiple","muted","name","nonce","noshade","novalidate","nowrap","open","optimum","part","pattern","placeholder","playsinline","popover","popovertarget","popovertargetaction","poster","preload","pubdate","radiogroup","readonly","rel","required","rev","reversed","role","rows","rowspan","spellcheck","scope","selected","shape","size","sizes","slot","span","srclang","start","src","srcset","step","style","summary","tabindex","title","translate","type","usemap","valign","value","width","wrap","xmlns"]),Zn=Tt(["accent-height","accumulate","additive","alignment-baseline","amplitude","ascent","attributename","attributetype","azimuth","basefrequency","baseline-shift","begin","bias","by","class","clip","clippathunits","clip-path","clip-rule","color","color-interpolation","color-interpolation-filters","color-profile","color-rendering","cx","cy","d","dx","dy","diffuseconstant","direction","display","divisor","dur","edgemode","elevation","end","exponent","fill","fill-opacity","fill-rule","filter","filterunits","flood-color","flood-opacity","font-family","font-size","font-size-adjust","font-stretch","font-style","font-variant","font-weight","fx","fy","g1","g2","glyph-name","glyphref","gradientunits","gradienttransform","height","href","id","image-rendering","in","in2","intercept","k","k1","k2","k3","k4","kerning","keypoints","keysplines","keytimes","lang","lengthadjust","letter-spacing","kernelmatrix","kernelunitlength","lighting-color","local","marker-end","marker-mid","marker-start","markerheight","markerunits","markerwidth","maskcontentunits","maskunits","max","mask","mask-type","media","method","mode","min","name","numoctaves","offset","operator","opacity","order","orient","orientation","origin","overflow","paint-order","path","pathlength","patterncontentunits","patterntransform","patternunits","points","preservealpha","preserveaspectratio","primitiveunits","r","rx","ry","radius","refx","refy","repeatcount","repeatdur","restart","result","rotate","scale","seed","shape-rendering","slope","specularconstant","specularexponent","spreadmethod","startoffset","stddeviation","stitchtiles","stop-color","stop-opacity","stroke-dasharray","stroke-dashoffset","stroke-linecap","stroke-linejoin","stroke-miterlimit","stroke-opacity","stroke","stroke-width","style","surfacescale","systemlanguage","tabindex","tablevalues","targetx","targety","transform","transform-origin","text-anchor","text-decoration","text-rendering","textlength","type","u1","u2","unicode","values","viewbox","visibility","version","vert-adv-y","vert-origin-x","vert-origin-y","width","word-spacing","wrap","writing-mode","xchannelselector","ychannelselector","x","x1","x2","xmlns","y","y1","y2","z","zoomandpan"]),er=Tt(["accent","accentunder","align","bevelled","close","columnalign","columnlines","columnspacing","columnspan","denomalign","depth","dir","display","displaystyle","encoding","fence","frame","height","href","id","largeop","length","linethickness","lquote","lspace","mathbackground","mathcolor","mathsize","mathvariant","maxsize","minsize","movablelimits","notation","numalign","open","rowalign","rowlines","rowspacing","rowspan","rspace","rquote","scriptlevel","scriptminsize","scriptsizemultiplier","selection","separator","separators","stretchy","subscriptshift","supscriptshift","symmetric","voffset","width","xmlns"]),an=Tt(["xlink:href","xml:id","xlink:title","xml:space","xmlns:xlink"]),Ra=Mt(/{{[\\w\\W]*|^[\\w\\W]*}}/g),Ia=Mt(/<%[\\w\\W]*|^[\\w\\W]*%>/g),Ma=Mt(/\\${[\\w\\W]*/g),Ca=Mt(/^data-[\\-\\w.\\u00B7-\\uFFFF]+$/),Da=Mt(/^aria-[\\-\\w]+$/),nr=Mt(/^(?:(?:(?:f|ht)tps?|mailto|tel|callto|sms|cid|xmpp|matrix):|[^a-z]|[a-z+.\\-]+(?:[^a-z+.\\-:]|$))/i),Oa=Mt(/^(?:\\w+script|data):/i),La=Mt(/[\\u0000-\\u0020\\u00A0\\u1680\\u180E\\u2000-\\u2029\\u205F\\u3000]/g),Fa=Mt(/^html$/i),ka=Mt(/^[a-z][.\\w]*(-[.\\w]+)+$/i),zt={element:1,attribute:2,text:3,cdataSection:4,entityReference:5,entityNode:6,progressingInstruction:7,comment:8,document:9,documentType:10,documentFragment:11,notation:12},Pa=function(){return typeof window>"u"?null:window},za=function(t,n){if(typeof t!="object"||typeof t.createPolicy!="function")return null;let i=null;const r="data-tt-policy-suffix";n&&n.hasAttribute(r)&&(i=n.getAttribute(r));const o="dompurify"+(i?"#"+i:"");try{return t.createPolicy(o,{createHTML(a){return a},createScriptURL(a){return a}})}catch{return console.warn("TrustedTypes policy "+o+" could not be created."),null}},ir=function(){return{afterSanitizeAttributes:[],afterSanitizeElements:[],afterSanitizeShadowDOM:[],beforeSanitizeAttributes:[],beforeSanitizeElements:[],beforeSanitizeShadowDOM:[],uponSanitizeAttribute:[],uponSanitizeElement:[],uponSanitizeShadowNode:[]}};function rr(){let e=arguments.length>0&&arguments[0]!==void 0?arguments[0]:Pa();const t=L=>rr(L);if(t.version="3.4.7",t.removed=[],!e||!e.document||e.document.nodeType!==zt.document||!e.Element)return t.isSupported=!1,t;let n=e.document;const i=n,r=i.currentScript;e.DocumentFragment;const o=e.HTMLTemplateElement,a=e.Node,s=e.Element,c=e.NodeFilter,l=e.NamedNodeMap;l===void 0&&(e.NamedNodeMap||e.MozNamedAttrMap),e.HTMLFormElement;const f=e.DOMParser,T=e.trustedTypes,d=s.prototype,m=Pt(d,"cloneNode"),A=Pt(d,"remove"),b=Pt(d,"nextSibling"),g=Pt(d,"childNodes"),w=Pt(d,"parentNode"),_=Pt(d,"shadowRoot"),R=Pt(d,"attributes"),y=a&&a.prototype?Pt(a.prototype,"nodeType"):null,C=a&&a.prototype?Pt(a.prototype,"nodeName"):null;if(typeof o=="function"){const L=n.createElement("template");L.content&&L.content.ownerDocument&&(n=L.content.ownerDocument)}let O,G="";const P=n,F=P.implementation,z=P.createNodeIterator,X=P.createDocumentFragment,nt=P.getElementsByTagName,N=i.importNode;let D=ir();t.isSupported=typeof ji=="function"&&typeof w=="function"&&F&&F.createHTMLDocument!==void 0;const K=Ra,j=Ia,I=Ma,B=Ca,Z=Da,ut=Oa,mt=La,it=ka;let H=nr,q=null;const Qt=W({},[...Qi,...Xn,...Yn,...qn,...Ji]);let J=null;const Jt=W({},[...tr,...Zn,...er,...an]);let U=Object.seal(ye(null,{tagNameCheck:{writable:!0,configurable:!1,enumerable:!0,value:null},attributeNameCheck:{writable:!0,configurable:!1,enumerable:!0,value:null},allowCustomizedBuiltInElements:{writable:!0,configurable:!1,enumerable:!0,value:!1}})),St=null,te=null;const Dt=Object.seal(ye(null,{tagCheck:{writable:!0,configurable:!1,enumerable:!0,value:null},attributeCheck:{writable:!0,configurable:!1,enumerable:!0,value:null}}));let Ge=!0,Be=!0,Sn=!1,wn=!0,Gt=!1,ce=!0,Wt=!1,Ue=!1,ee=!1,Bt=!1,jt=!1,At=!1,He=!0,We=!1;const An="user-content-";let je=!0,Ut=!1,Kt={},vt=null;const we=W({},["annotation-xml","audio","colgroup","desc","foreignobject","head","iframe","math","mi","mn","mo","ms","mtext","noembed","noframes","noscript","plaintext","script","style","svg","template","thead","title","video","xmp"]);let Ke=null;const vn=W({},["audio","video","img","source","image","track"]);let Ve=null;const Ht=W({},["alt","class","for","id","label","name","pattern","placeholder","role","summary","title","value","style","xmlns"]),Vt="http://www.w3.org/1998/Math/MathML",Ae="http://www.w3.org/2000/svg",Et="http://www.w3.org/1999/xhtml";let ne=Et,ve=!1,$t=null;const ri=W({},[Vt,Ae,Et],$n);let Ot=W({},["mi","mo","mn","ms","mtext"]),$e=W({},["annotation-xml"]);const oi=W({},["title","style","font","a","script"]);let ue=null;const si=["application/xhtml+xml","text/html"],ai="text/html";let tt=null,ie=null;const li=n.createElement("form"),En=function(h){return h instanceof RegExp||h instanceof Function},Xe=function(){let h=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{};if(ie&&ie===h)return;(!h||typeof h!="object")&&(h={}),h=gt(h),ue=si.indexOf(h.PARSER_MEDIA_TYPE)===-1?ai:h.PARSER_MEDIA_TYPE,tt=ue==="application/xhtml+xml"?$n:De,q=ct(h,"ALLOWED_TAGS")&&bt(h.ALLOWED_TAGS)?W({},h.ALLOWED_TAGS,tt):Qt,J=ct(h,"ALLOWED_ATTR")&&bt(h.ALLOWED_ATTR)?W({},h.ALLOWED_ATTR,tt):Jt,$t=ct(h,"ALLOWED_NAMESPACES")&&bt(h.ALLOWED_NAMESPACES)?W({},h.ALLOWED_NAMESPACES,$n):ri,Ve=ct(h,"ADD_URI_SAFE_ATTR")&&bt(h.ADD_URI_SAFE_ATTR)?W(gt(Ht),h.ADD_URI_SAFE_ATTR,tt):Ht,Ke=ct(h,"ADD_DATA_URI_TAGS")&&bt(h.ADD_DATA_URI_TAGS)?W(gt(vn),h.ADD_DATA_URI_TAGS,tt):vn,vt=ct(h,"FORBID_CONTENTS")&&bt(h.FORBID_CONTENTS)?W({},h.FORBID_CONTENTS,tt):we,St=ct(h,"FORBID_TAGS")&&bt(h.FORBID_TAGS)?W({},h.FORBID_TAGS,tt):gt({}),te=ct(h,"FORBID_ATTR")&&bt(h.FORBID_ATTR)?W({},h.FORBID_ATTR,tt):gt({}),Kt=ct(h,"USE_PROFILES")?h.USE_PROFILES&&typeof h.USE_PROFILES=="object"?gt(h.USE_PROFILES):h.USE_PROFILES:!1,Ge=h.ALLOW_ARIA_ATTR!==!1,Be=h.ALLOW_DATA_ATTR!==!1,Sn=h.ALLOW_UNKNOWN_PROTOCOLS||!1,wn=h.ALLOW_SELF_CLOSE_IN_ATTR!==!1,Gt=h.SAFE_FOR_TEMPLATES||!1,ce=h.SAFE_FOR_XML!==!1,Wt=h.WHOLE_DOCUMENT||!1,Bt=h.RETURN_DOM||!1,jt=h.RETURN_DOM_FRAGMENT||!1,At=h.RETURN_TRUSTED_TYPE||!1,ee=h.FORCE_BODY||!1,He=h.SANITIZE_DOM!==!1,We=h.SANITIZE_NAMED_PROPS||!1,je=h.KEEP_CONTENT!==!1,Ut=h.IN_PLACE||!1,H=Ea(h.ALLOWED_URI_REGEXP)?h.ALLOWED_URI_REGEXP:nr,ne=typeof h.NAMESPACE=="string"?h.NAMESPACE:Et,Ot=ct(h,"MATHML_TEXT_INTEGRATION_POINTS")&&h.MATHML_TEXT_INTEGRATION_POINTS&&typeof h.MATHML_TEXT_INTEGRATION_POINTS=="object"?gt(h.MATHML_TEXT_INTEGRATION_POINTS):W({},["mi","mo","mn","ms","mtext"]),$e=ct(h,"HTML_INTEGRATION_POINTS")&&h.HTML_INTEGRATION_POINTS&&typeof h.HTML_INTEGRATION_POINTS=="object"?gt(h.HTML_INTEGRATION_POINTS):W({},["annotation-xml"]);const v=ct(h,"CUSTOM_ELEMENT_HANDLING")&&h.CUSTOM_ELEMENT_HANDLING&&typeof h.CUSTOM_ELEMENT_HANDLING=="object"?gt(h.CUSTOM_ELEMENT_HANDLING):ye(null);if(U=ye(null),ct(v,"tagNameCheck")&&En(v.tagNameCheck)&&(U.tagNameCheck=v.tagNameCheck),ct(v,"attributeNameCheck")&&En(v.attributeNameCheck)&&(U.attributeNameCheck=v.attributeNameCheck),ct(v,"allowCustomizedBuiltInElements")&&typeof v.allowCustomizedBuiltInElements=="boolean"&&(U.allowCustomizedBuiltInElements=v.allowCustomizedBuiltInElements),Gt&&(Be=!1),jt&&(Bt=!0),Kt&&(q=W({},Ji),J=ye(null),Kt.html===!0&&(W(q,Qi),W(J,tr)),Kt.svg===!0&&(W(q,Xn),W(J,Zn),W(J,an)),Kt.svgFilters===!0&&(W(q,Yn),W(J,Zn),W(J,an)),Kt.mathMl===!0&&(W(q,qn),W(J,er),W(J,an))),Dt.tagCheck=null,Dt.attributeCheck=null,ct(h,"ADD_TAGS")&&(typeof h.ADD_TAGS=="function"?Dt.tagCheck=h.ADD_TAGS:bt(h.ADD_TAGS)&&(q===Qt&&(q=gt(q)),W(q,h.ADD_TAGS,tt))),ct(h,"ADD_ATTR")&&(typeof h.ADD_ATTR=="function"?Dt.attributeCheck=h.ADD_ATTR:bt(h.ADD_ATTR)&&(J===Jt&&(J=gt(J)),W(J,h.ADD_ATTR,tt))),ct(h,"ADD_URI_SAFE_ATTR")&&bt(h.ADD_URI_SAFE_ATTR)&&W(Ve,h.ADD_URI_SAFE_ATTR,tt),ct(h,"FORBID_CONTENTS")&&bt(h.FORBID_CONTENTS)&&(vt===we&&(vt=gt(vt)),W(vt,h.FORBID_CONTENTS,tt)),ct(h,"ADD_FORBID_CONTENTS")&&bt(h.ADD_FORBID_CONTENTS)&&(vt===we&&(vt=gt(vt)),W(vt,h.ADD_FORBID_CONTENTS,tt)),je&&(q["#text"]=!0),Wt&&W(q,["html","head","body"]),q.table&&(W(q,["tbody"]),delete St.tbody),h.TRUSTED_TYPES_POLICY){if(typeof h.TRUSTED_TYPES_POLICY.createHTML!="function")throw Le(\'TRUSTED_TYPES_POLICY configuration option must provide a "createHTML" hook.\');if(typeof h.TRUSTED_TYPES_POLICY.createScriptURL!="function")throw Le(\'TRUSTED_TYPES_POLICY configuration option must provide a "createScriptURL" hook.\');O=h.TRUSTED_TYPES_POLICY,G=O.createHTML("")}else O===void 0&&(O=za(T,r)),O!==null&&typeof G=="string"&&(G=O.createHTML(""));(D.uponSanitizeElement.length>0||D.uponSanitizeAttribute.length>0)&&q===Qt&&(q=gt(q)),D.uponSanitizeAttribute.length>0&&J===Jt&&(J=gt(J)),Tt&&Tt(h),ie=h},xn=W({},[...Xn,...Yn,...xa]),Xt=W({},[...qn,...Na]),ci=function(h){let v=w(h);(!v||!v.tagName)&&(v={namespaceURI:ne,tagName:"template"});const M=De(h.tagName),Q=De(v.tagName);return $t[h.namespaceURI]?h.namespaceURI===Ae?v.namespaceURI===Et?M==="svg":v.namespaceURI===Vt?M==="svg"&&(Q==="annotation-xml"||Ot[Q]):!!xn[M]:h.namespaceURI===Vt?v.namespaceURI===Et?M==="math":v.namespaceURI===Ae?M==="math"&&$e[Q]:!!Xt[M]:h.namespaceURI===Et?v.namespaceURI===Ae&&!$e[Q]||v.namespaceURI===Vt&&!Ot[Q]?!1:!Xt[M]&&(oi[M]||!xn[M]):!!(ue==="application/xhtml+xml"&&$t[h.namespaceURI]):!1},xt=function(h){Te(t.removed,{element:h});try{w(h).removeChild(h)}catch{A(h)}},Yt=function(h,v){try{Te(t.removed,{attribute:v.getAttributeNode(h),from:v})}catch{Te(t.removed,{attribute:null,from:v})}if(v.removeAttribute(h),h==="is")if(Bt||jt)try{xt(v)}catch{}else try{v.setAttribute(h,"")}catch{}},Nn=function(h){let v=null,M=null;if(ee)h="<remove></remove>"+h;else{const rt=Xi(h,/^[\\r\\n\\t ]+/);M=rt&&rt[0]}ue==="application/xhtml+xml"&&ne===Et&&(h=\'<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body>\'+h+"</body></html>");const Q=O?O.createHTML(h):h;if(ne===Et)try{v=new f().parseFromString(Q,ue)}catch{}if(!v||!v.documentElement){v=F.createDocument(ne,"template",null);try{v.documentElement.innerHTML=ve?G:Q}catch{}}const V=v.body||v.documentElement;return h&&M&&V.insertBefore(n.createTextNode(M),V.childNodes[0]||null),ne===Et?nt.call(v,Wt?"html":"body")[0]:Wt?v.documentElement:V},Rn=function(h){return z.call(h.ownerDocument||h,h,c.SHOW_ELEMENT|c.SHOW_COMMENT|c.SHOW_TEXT|c.SHOW_PROCESSING_INSTRUCTION|c.SHOW_CDATA_SECTION,null)},qt=function(h){h.normalize();const v=z.call(h.ownerDocument||h,h,c.SHOW_TEXT|c.SHOW_COMMENT|c.SHOW_CDATA_SECTION|c.SHOW_PROCESSING_INSTRUCTION,null);let M=v.nextNode();for(;M;){let Q=M.data;_e([K,j,I],V=>{Q=be(Q,V," ")}),M.data=Q,M=v.nextNode()}},Ee=function(h){const v=C?C(h):null;return typeof v!="string"||tt(v)!=="form"?!1:typeof h.nodeName!="string"||typeof h.textContent!="string"||typeof h.removeChild!="function"||h.attributes!==R(h)||typeof h.removeAttribute!="function"||typeof h.setAttribute!="function"||typeof h.namespaceURI!="string"||typeof h.insertBefore!="function"||typeof h.hasChildNodes!="function"||h.nodeType!==y(h)||h.childNodes!==g(h)},he=function(h){if(!y||typeof h!="object"||h===null)return!1;try{return y(h)===zt.documentFragment}catch{return!1}},xe=function(h){if(!y||typeof h!="object"||h===null)return!1;try{return typeof y(h)=="number"}catch{return!1}};function Lt(L,h,v){_e(L,M=>{M.call(t,h,v,ie)})}const In=function(h){let v=null;if(Lt(D.beforeSanitizeElements,h,null),Ee(h))return xt(h),!0;const M=tt(h.nodeName);if(Lt(D.uponSanitizeElement,h,{tagName:M,allowedTags:q}),ce&&h.hasChildNodes()&&!xe(h.firstElementChild)&&pt(/<[/\\w!]/g,h.innerHTML)&&pt(/<[/\\w!]/g,h.textContent)||ce&&h.namespaceURI===Et&&M==="style"&&xe(h.firstElementChild)||h.nodeType===zt.progressingInstruction||ce&&h.nodeType===zt.comment&&pt(/<[/\\w]/g,h.data))return xt(h),!0;if(St[M]||!(Dt.tagCheck instanceof Function&&Dt.tagCheck(M))&&!q[M]){if(!St[M]&&Cn(M)&&(U.tagNameCheck instanceof RegExp&&pt(U.tagNameCheck,M)||U.tagNameCheck instanceof Function&&U.tagNameCheck(M)))return!1;if(je&&!vt[M]){const V=w(h),rt=g(h);if(rt&&V){const wt=rt.length;for(let Ct=wt-1;Ct>=0;--Ct){const Nt=m(rt[Ct],!0);V.insertBefore(Nt,b(h))}}}return xt(h),!0}return(y?y(h):h.nodeType)===zt.element&&!ci(h)||(M==="noscript"||M==="noembed"||M==="noframes")&&pt(/<\\/no(script|embed|frames)/i,h.innerHTML)?(xt(h),!0):(Gt&&h.nodeType===zt.text&&(v=h.textContent,_e([K,j,I],V=>{v=be(v,V," ")}),h.textContent!==v&&(Te(t.removed,{element:h.cloneNode()}),h.textContent=v)),Lt(D.afterSanitizeElements,h,null),!1)},Mn=function(h,v,M){if(te[v]||He&&(v==="id"||v==="name")&&(M in n||M in li))return!1;const Q=J[v]||Dt.attributeCheck instanceof Function&&Dt.attributeCheck(v,h);if(!(Be&&!te[v]&&pt(B,v))){if(!(Ge&&pt(Z,v))){if(!Q||te[v]){if(!(Cn(h)&&(U.tagNameCheck instanceof RegExp&&pt(U.tagNameCheck,h)||U.tagNameCheck instanceof Function&&U.tagNameCheck(h))&&(U.attributeNameCheck instanceof RegExp&&pt(U.attributeNameCheck,v)||U.attributeNameCheck instanceof Function&&U.attributeNameCheck(v,h))||v==="is"&&U.allowCustomizedBuiltInElements&&(U.tagNameCheck instanceof RegExp&&pt(U.tagNameCheck,M)||U.tagNameCheck instanceof Function&&U.tagNameCheck(M))))return!1}else if(!Ve[v]){if(!pt(H,be(M,mt,""))){if(!((v==="src"||v==="xlink:href"||v==="href")&&h!=="script"&&Yi(M,"data:")===0&&Ke[h])){if(!(Sn&&!pt(ut,be(M,mt,"")))){if(M)return!1}}}}}}return!0},Ye=W({},["annotation-xml","color-profile","font-face","font-face-format","font-face-name","font-face-src","font-face-uri","missing-glyph"]),Cn=function(h){return!Ye[De(h)]&&pt(it,h)},fe=function(h){Lt(D.beforeSanitizeAttributes,h,null);const v=h.attributes;if(!v||Ee(h))return;const M={attrName:"",attrValue:"",keepAttr:!0,allowedAttributes:J,forceKeepAttr:void 0};let Q=v.length;for(;Q--;){const V=v[Q],rt=V.name,wt=V.namespaceURI,Ct=V.value,Nt=tt(rt),qe=Ct;let ft=rt==="value"?qe:Ta(qe);if(M.attrName=Nt,M.attrValue=ft,M.keepAttr=!0,M.forceKeepAttr=void 0,Lt(D.uponSanitizeAttribute,h,M),ft=M.attrValue,We&&(Nt==="id"||Nt==="name")&&Yi(ft,An)!==0&&(Yt(rt,h),ft=An+ft),ce&&pt(/((--!?|])>)|<\\/(style|script|title|xmp|textarea|noscript|iframe|noembed|noframes)/i,ft)){Yt(rt,h);continue}if(Nt==="attributename"&&Xi(ft,"href")){Yt(rt,h);continue}if(M.forceKeepAttr)continue;if(!M.keepAttr){Yt(rt,h);continue}if(!wn&&pt(/\\/>/i,ft)){Yt(rt,h);continue}Gt&&_e([K,j,I],On=>{ft=be(ft,On," ")});const Dn=tt(h.nodeName);if(!Mn(Dn,Nt,ft)){Yt(rt,h);continue}if(O&&typeof T=="object"&&typeof T.getAttributeType=="function"&&!wt)switch(T.getAttributeType(Dn,Nt)){case"TrustedHTML":{ft=O.createHTML(ft);break}case"TrustedScriptURL":{ft=O.createScriptURL(ft);break}}if(ft!==qe)try{wt?h.setAttributeNS(wt,rt,ft):h.setAttribute(rt,ft),Ee(h)?xt(h):$i(t.removed)}catch{Yt(rt,h)}}Lt(D.afterSanitizeAttributes,h,null)},re=function(h){let v=null;const M=Rn(h);for(Lt(D.beforeSanitizeShadowDOM,h,null);v=M.nextNode();)if(Lt(D.uponSanitizeShadowNode,v,null),In(v),fe(v),he(v.content)&&re(v.content),(y?y(v):v.nodeType)===zt.element){const V=_?_(v):v.shadowRoot;he(V)&&(oe(V),re(V))}Lt(D.afterSanitizeShadowDOM,h,null)},oe=function(h){const v=y?y(h):h.nodeType;if(v===zt.element){const V=_?_(h):h.shadowRoot;he(V)&&(oe(V),re(V))}const M=g?g(h):h.childNodes;if(!M)return;const Q=[];_e(M,V=>{Te(Q,V)});for(const V of Q)oe(V);if(v===zt.element){const V=C?C(h):null;if(typeof V=="string"&&tt(V)==="template"){const rt=h.content;he(rt)&&oe(rt)}}};return t.sanitize=function(L){let h=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},v=null,M=null,Q=null,V=null;if(ve=!L,ve&&(L="<!-->"),typeof L!="string"&&!xe(L)&&(L=va(L),typeof L!="string"))throw Le("dirty is not a string, aborting");if(!t.isSupported)return L;if(Ue||Xe(h),t.removed=[],typeof L=="string"&&(Ut=!1),Ut){const Ct=C?C(L):L.nodeName;if(typeof Ct=="string"){const Nt=tt(Ct);if(!q[Nt]||St[Nt])throw Le("root node is forbidden and cannot be sanitized in-place")}if(Ee(L))throw Le("root node is clobbered and cannot be sanitized in-place");oe(L)}else if(xe(L))v=Nn("<!---->"),M=v.ownerDocument.importNode(L,!0),M.nodeType===zt.element&&M.nodeName==="BODY"||M.nodeName==="HTML"?v=M:v.appendChild(M),oe(M);else{if(!Bt&&!Gt&&!Wt&&L.indexOf("<")===-1)return O&&At?O.createHTML(L):L;if(v=Nn(L),!v)return Bt?null:At?G:""}v&&ee&&xt(v.firstChild);const rt=Rn(Ut?L:v);for(;Q=rt.nextNode();)In(Q),fe(Q),he(Q.content)&&re(Q.content);if(Ut)return Gt&&qt(L),L;if(Bt){if(Gt&&qt(v),jt)for(V=X.call(v.ownerDocument);v.firstChild;)V.appendChild(v.firstChild);else V=v;return(J.shadowroot||J.shadowrootmode)&&(V=N.call(i,V,!0)),V}let wt=Wt?v.outerHTML:v.innerHTML;return Wt&&q["!doctype"]&&v.ownerDocument&&v.ownerDocument.doctype&&v.ownerDocument.doctype.name&&pt(Fa,v.ownerDocument.doctype.name)&&(wt="<!DOCTYPE "+v.ownerDocument.doctype.name+`>\n`+wt),Gt&&_e([K,j,I],Ct=>{wt=be(wt,Ct," ")}),O&&At?O.createHTML(wt):wt},t.setConfig=function(){let L=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{};Xe(L),Ue=!0},t.clearConfig=function(){ie=null,Ue=!1},t.isValidAttribute=function(L,h,v){ie||Xe({});const M=tt(L),Q=tt(h);return Mn(M,Q,v)},t.addHook=function(L,h){typeof h=="function"&&Te(D[L],h)},t.removeHook=function(L,h){if(h!==void 0){const v=ya(D[L],h);return v===-1?void 0:_a(D[L],v,1)[0]}return $i(D[L])},t.removeHooks=function(L){D[L]=[]},t.removeAllHooks=function(){D=ir()},t}rr();function or(e=8,t="id-"){const n="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",i=n+"0123456789-_";let r=n.charAt(Math.floor(Math.random()*n.length));for(let o=1;o<e;o++)r+=i.charAt(Math.floor(Math.random()*i.length));return`${t}${r}`}function Ga(e,t,n,i){const r=Math.max(Math.abs(n)/e,Math.abs(i)/t);return r===0?e:1/r}function ln(e,t=new WeakSet){if(typeof e=="function")return;if(e===null||typeof e!="object")return e;const n=e;if(t.has(n))return;if(t.add(n),Array.isArray(e))return e.map(r=>ln(r,t));if(Object.getPrototypeOf(e)!==Object.prototype)return e;const i={};for(const[r,o]of Object.entries(e))typeof o!="function"&&(i[r]=ln(o,t));return i}let sr=class Lr{constructor(t,n,i,r=or(),o=[]){E(this,"id");E(this,"data");E(this,"children");E(this,"style");E(this,"edgesOut");E(this,"edgesIn");E(this,"defaultCircleRadius",10);E(this,"x");E(this,"y");E(this,"vx");E(this,"vy");E(this,"fx");E(this,"fy");E(this,"weight");E(this,"frozen");E(this,"visible");E(this,"expanded");E(this,"isChild");E(this,"childrenDepth");E(this,"isParent");E(this,"parentNode");E(this,"_original_object");E(this,"_deepest_node_clone");E(this,"_subgraph");E(this,"_circleRadius",this.defaultCircleRadius);E(this,"_circleRadiusCollapsed",this.defaultCircleRadius);E(this,"_border");E(this,"_dirty");E(this,"domID");this.id=t,this.domID=r,this.data=n??{},this.style=i??{},this.children=[],this.isParent=!1,this.setChildren(o),this._dirty=!0,this.frozen=!1,this.visible=!0,this.expanded=!1,this.isChild=!1,this.childrenDepth=0,this.edgesOut=new Set,this.edgesIn=new Set}getData(){return this.data}setData(t){this.data=t,this.markDirty()}updateData(t){this.data={...this.data,...t},this.markDirty()}registerEdgeOut(t){this.edgesOut.add(t)}registerEdgeIn(t){this.edgesIn.add(t)}emptyEdges(){this.edgesOut.clear(),this.edgesIn.clear()}getConnectedNodes(){return[...this.edgesOut].map(t=>t.to)}getConnectingNodes(){return[...this.edgesIn].map(t=>t.from)}getEdgesOut(){return[...this.edgesOut]}getEdgesIn(){return[...this.edgesIn]}getStyle(){return this.style}setStyle(t){this.style=t,this.markDirty()}updateStyle(t){this.style={...this.style,...t},this.markDirty()}getGraphElement(){return document?document.getElementById(`node-${this.domID}`):null}toDict(t=!1){const n={id:this.id,data:this.data,style:this.style,weight:this.weight};return t||(n.x=this.x,n.y=this.y,n.vx=this.vx,n.vy=this.vy,n.fx=this.fx,n.fy=this.fy),this.hasChildren()&&(n.children=this.children.map(i=>i.toDict(t))),n}toSimulationDTO(){return{id:this.id,data:this.data,style:ln(this.style),weight:this.weight,_circleRadius:this._circleRadius,x:this.x,y:this.y,vx:this.vx,vy:this.vy,fx:this.fx,fy:this.fy}}clone(){const t={...this.data},n={...this.style},i=new Lr(this.id,t,n);return i.x=this.x,i.y=this.y,i.vx=this.vx,i.vy=this.vy,i.fx=this.fx,i.fy=this.fy,i.weight=this.weight,i.frozen=this.frozen,i.visible=this.visible,i.expanded=this.expanded,i.isChild=this.isChild,i.childrenDepth=this.childrenDepth,i.isParent=this.isParent,i.parentNode=this.parentNode,i._circleRadius=this._circleRadius,i.children=this.children.map(r=>r.clone()),i}markDirty(){this._dirty=!0}clearDirty(){this._dirty=!1}isDirty(){return this._dirty}freeze(){this.frozen=!0,this.fx=this.x,this.fy=this.y}unfreeze(){this.frozen=!1,this.fx=void 0,this.fy=void 0}toggleVisibility(t){t?this.show():this.hide(),this.markDirty()}show(){this.visible=!0}hide(){this.visible=!1}toggleExpand(t){t===void 0?this.expanded?this.collapse():this.expand():t?this.expand():this.collapse(),this.markDirty()}expand(){this.expanded=!0,this._original_object&&(this._original_object.expanded=!0)}collapse(){this.expanded=!1,this._original_object&&(this._original_object.expanded=!1)}degree(){return this.edgesOut.size+this.edgesIn.size}ancestorChain(){const t=[];let n=this.parentNode;for(;n;)t.unshift(n),n=n.parentNode;return t}canvasRepresentative(){for(const t of this.ancestorChain())if(!t.expanded)return t;return this}setCircleRadius(t){this._circleRadius=t,this._border=void 0}getCircleRadius(){return this._circleRadius}setCircleRadiusCollapsed(t){this._circleRadiusCollapsed=t}getCircleRadiusCollapsed(){return this._circleRadiusCollapsed}setBorderBox(t,n){this._border={halfWidth:t/2,halfHeight:n/2}}getBorderBox(t=0){if(this._border)return{halfWidth:this._border.halfWidth+t,halfHeight:this._border.halfHeight+t}}getBorderDistance(t,n,i=0){const r=this._border;return r?Ga(r.halfWidth+i,r.halfHeight+i,t,n):this._circleRadius+i}setChildren(t){this.children=t,this.hasChildren()?this.isParent=!0:this.isParent=!1}hasChildren(){return this.children.length>0}markAsChild(t,n){this.isChild=!0,this.childrenDepth=n,this.parentNode=t}markAsParent(){this.isParent=!0}setSubgraph(t){this._subgraph=t}getSubgraph(){return this._subgraph}setOriginalObject(t){this._original_object=t}getOriginalObject(){return this._original_object}setDeepestNodeClone(t){this._deepest_node_clone=t}getDeepestNodeClone(){return this._deepest_node_clone}};class cn{constructor(t,n,i,r,o,a=null,s){E(this,"id");E(this,"from");E(this,"to");E(this,"directed");E(this,"data");E(this,"style");E(this,"visible");E(this,"layerVisible");E(this,"visibleIgnoringLayer");E(this,"representedEdges");E(this,"isSynthetic");E(this,"isCrossCluster");E(this,"syntheticTerminalNode");E(this,"syntheticSourceNode");E(this,"_original_object");E(this,"_subgraphFromNode");E(this,"_subgraphToNode");E(this,"_dirty");E(this,"domID");this.id=t,this.domID=or(),this.from=n,this.to=i,this.directed=a,this.data=r??{},this.style=o??{},this.visible=!0,this.layerVisible=!0,this.visibleIgnoringLayer=!0,this._dirty=!0,this.isSynthetic=s!==void 0,this.syntheticTerminalNode=s,this.from.registerEdgeOut(this),this.to.registerEdgeIn(this)}get source(){return this.from}get target(){return this.to}getData(){return this.data}setData(t){this.data=t,this.markDirty()}updateData(t){this.data={...this.data,...t},this.markDirty()}getStyle(){return this.style}getEdgeStyle(){var t;return((t=this.style)==null?void 0:t.edge)??{}}getLabelStyle(){var t;return((t=this.style)==null?void 0:t.label)??{}}setStyle(t){this.style=t,this.markDirty()}updateStyle(t){const n=this.style,i=t;this.style={...n,...i,edge:{...n.edge,...i.edge},label:{...n.label,...i.label}},this.markDirty()}getGraphElement(){return document?document.getElementById(`edge-${this.domID}`):null}setFrom(t){this.from=t}setTo(t){this.to=t}toDict(){return{id:this.id,from:this.from.id,to:this.to.id,data:this.data,style:this.style}}toSimulationDTO(){return{id:this.id,from:{id:this.from.id},to:{id:this.to.id},data:this.data,style:ln(this.style),directed:this.directed}}clone(){const t={...this.data},n={...this.style},i=new cn(this.id,this.from.clone(),this.to.clone(),t,n,this.directed);return i.visible=this.visible,i.layerVisible=this.layerVisible,i.visibleIgnoringLayer=this.visibleIgnoringLayer,i}markDirty(){this._dirty=!0}clearDirty(){this._dirty=!1}isDirty(){return this._dirty}toggleVisibility(t){t?this.show():this.hide(),this.markDirty()}show(){this.visibleIgnoringLayer=!0,this.visible=this.layerVisible}hide(){this.visibleIgnoringLayer=!1,this.visible=!1}setLayerVisible(t){if(this.layerVisible===t)return!1;this.layerVisible=t;const n=t&&this.visibleIgnoringLayer;return this.visible!==n&&(this.visible=n,this.markDirty()),!0}setOriginalObject(t){this._original_object=t}getOriginalObject(){return this._original_object}setSubgraphFromNode(t){this._subgraphFromNode=t}setSubgraphToNode(t){this._subgraphToNode=t}getSubgraphFromNode(){return this._subgraphFromNode}getSubgraphToNode(){return this._subgraphToNode}}function Ba(e){return new Worker(self.location.href,{name:e==null?void 0:e.name})}function Ua(){return new Ba}const Ha=(e,t,n,i,r)=>new Promise((o,a)=>{const s=Ua();s.postMessage({source:"simulation-worker-wrapper",nodes:e,edges:t,options:n,canvasBCR:i}),s.onmessage=c=>{const{type:l,progress:f,nodes:T,edges:d,elapsedTime:m}=c.data;if(l==="tick"&&typeof f=="number"){r==null||r(f,m);return}l==="done"&&(o({nodes:T,edges:d}),s.terminate())},s.onerror=a});var un=typeof globalThis<"u"?globalThis:typeof window<"u"?window:typeof global<"u"?global:typeof self<"u"?self:{};function Wa(e){return e&&e.__esModule&&Object.prototype.hasOwnProperty.call(e,"default")?e.default:e}var Fe={exports:{}};Fe.exports;var ar;function ja(){return ar||(ar=1,(function(e,t){var n=200,i="__lodash_hash_undefined__",r=800,o=16,a=9007199254740991,s="[object Arguments]",c="[object Array]",l="[object AsyncFunction]",f="[object Boolean]",T="[object Date]",d="[object Error]",m="[object Function]",A="[object GeneratorFunction]",b="[object Map]",g="[object Number]",w="[object Null]",_="[object Object]",R="[object Proxy]",y="[object RegExp]",C="[object Set]",O="[object String]",G="[object Undefined]",P="[object WeakMap]",F="[object ArrayBuffer]",z="[object DataView]",X="[object Float32Array]",nt="[object Float64Array]",N="[object Int8Array]",D="[object Int16Array]",K="[object Int32Array]",j="[object Uint8Array]",I="[object Uint8ClampedArray]",B="[object Uint16Array]",Z="[object Uint32Array]",ut=/[\\\\^$.*+?()[\\]{}|]/g,mt=/^\\[object .+?Constructor\\]$/,it=/^(?:0|[1-9]\\d*)$/,H={};H[X]=H[nt]=H[N]=H[D]=H[K]=H[j]=H[I]=H[B]=H[Z]=!0,H[s]=H[c]=H[F]=H[f]=H[z]=H[T]=H[d]=H[m]=H[b]=H[g]=H[_]=H[y]=H[C]=H[O]=H[P]=!1;var q=typeof un=="object"&&un&&un.Object===Object&&un,Qt=typeof self=="object"&&self&&self.Object===Object&&self,J=q||Qt||Function("return this")(),Jt=t&&!t.nodeType&&t,U=Jt&&!0&&e&&!e.nodeType&&e,St=U&&U.exports===Jt,te=St&&q.process,Dt=(function(){try{var u=U&&U.require&&U.require("util").types;return u||te&&te.binding&&te.binding("util")}catch{}})(),Ge=Dt&&Dt.isTypedArray;function Be(u,p,S){switch(S.length){case 0:return u.call(p);case 1:return u.call(p,S[0]);case 2:return u.call(p,S[0],S[1]);case 3:return u.call(p,S[0],S[1],S[2])}return u.apply(p,S)}function Sn(u,p){for(var S=-1,k=Array(u);++S<u;)k[S]=p(S);return k}function wn(u){return function(p){return u(p)}}function Gt(u,p){return u==null?void 0:u[p]}function ce(u,p){return function(S){return u(p(S))}}var Wt=Array.prototype,Ue=Function.prototype,ee=Object.prototype,Bt=J["__core-js_shared__"],jt=Ue.toString,At=ee.hasOwnProperty,He=(function(){var u=/[^.]+$/.exec(Bt&&Bt.keys&&Bt.keys.IE_PROTO||"");return u?"Symbol(src)_1."+u:""})(),We=ee.toString,An=jt.call(Object),je=RegExp("^"+jt.call(At).replace(ut,"\\\\$&").replace(/hasOwnProperty|(function).*?(?=\\\\\\()| for .+?(?=\\\\\\])/g,"$1.*?")+"$"),Ut=St?J.Buffer:void 0,Kt=J.Symbol,vt=J.Uint8Array;Ut&&Ut.allocUnsafe;var we=ce(Object.getPrototypeOf,Object),Ke=Object.create,vn=ee.propertyIsEnumerable,Ve=Wt.splice,Ht=Kt?Kt.toStringTag:void 0,Vt=(function(){try{var u=ui(Object,"defineProperty");return u({},"",{}),u}catch{}})(),Ae=Ut?Ut.isBuffer:void 0,Et=Math.max,ne=Date.now,ve=ui(J,"Map"),$t=ui(Object,"create"),ri=(function(){function u(){}return function(p){if(!de(p))return{};if(Ke)return Ke(p);u.prototype=p;var S=new u;return u.prototype=void 0,S}})();function Ot(u){var p=-1,S=u==null?0:u.length;for(this.clear();++p<S;){var k=u[p];this.set(k[0],k[1])}}function $e(){this.__data__=$t?$t(null):{},this.size=0}function oi(u){var p=this.has(u)&&delete this.__data__[u];return this.size-=p?1:0,p}function ue(u){var p=this.__data__;if($t){var S=p[u];return S===i?void 0:S}return At.call(p,u)?p[u]:void 0}function si(u){var p=this.__data__;return $t?p[u]!==void 0:At.call(p,u)}function ai(u,p){var S=this.__data__;return this.size+=this.has(u)?0:1,S[u]=$t&&p===void 0?i:p,this}Ot.prototype.clear=$e,Ot.prototype.delete=oi,Ot.prototype.get=ue,Ot.prototype.has=si,Ot.prototype.set=ai;function tt(u){var p=-1,S=u==null?0:u.length;for(this.clear();++p<S;){var k=u[p];this.set(k[0],k[1])}}function ie(){this.__data__=[],this.size=0}function li(u){var p=this.__data__,S=fe(p,u);if(S<0)return!1;var k=p.length-1;return S==k?p.pop():Ve.call(p,S,1),--this.size,!0}function En(u){var p=this.__data__,S=fe(p,u);return S<0?void 0:p[S][1]}function Xe(u){return fe(this.__data__,u)>-1}function xn(u,p){var S=this.__data__,k=fe(S,u);return k<0?(++this.size,S.push([u,p])):S[k][1]=p,this}tt.prototype.clear=ie,tt.prototype.delete=li,tt.prototype.get=En,tt.prototype.has=Xe,tt.prototype.set=xn;function Xt(u){var p=-1,S=u==null?0:u.length;for(this.clear();++p<S;){var k=u[p];this.set(k[0],k[1])}}function ci(){this.size=0,this.__data__={hash:new Ot,map:new(ve||tt),string:new Ot}}function xt(u){var p=Ln(this,u).delete(u);return this.size-=p?1:0,p}function Yt(u){return Ln(this,u).get(u)}function Nn(u){return Ln(this,u).has(u)}function Rn(u,p){var S=Ln(this,u),k=S.size;return S.set(u,p),this.size+=S.size==k?0:1,this}Xt.prototype.clear=ci,Xt.prototype.delete=xt,Xt.prototype.get=Yt,Xt.prototype.has=Nn,Xt.prototype.set=Rn;function qt(u){var p=this.__data__=new tt(u);this.size=p.size}function Ee(){this.__data__=new tt,this.size=0}function he(u){var p=this.__data__,S=p.delete(u);return this.size=p.size,S}function xe(u){return this.__data__.get(u)}function Lt(u){return this.__data__.has(u)}function In(u,p){var S=this.__data__;if(S instanceof tt){var k=S.__data__;if(!ve||k.length<n-1)return k.push([u,p]),this.size=++S.size,this;S=this.__data__=new Xt(k)}return S.set(u,p),this.size=S.size,this}qt.prototype.clear=Ee,qt.prototype.delete=he,qt.prototype.get=xe,qt.prototype.has=Lt,qt.prototype.set=In;function Mn(u,p){var S=di(u),k=!S&&fi(u),Y=!S&&!k&&Rr(u),ot=!S&&!k&&!Y&&Mr(u),st=S||k||Y||ot,et=st?Sn(u.length,String):[],at=et.length;for(var Ft in u)st&&(Ft=="length"||Y&&(Ft=="offset"||Ft=="parent")||ot&&(Ft=="buffer"||Ft=="byteLength"||Ft=="byteOffset")||xr(Ft,at))||et.push(Ft);return et}function Ye(u,p,S){(S!==void 0&&!Fn(u[p],S)||S===void 0&&!(p in u))&&re(u,p,S)}function Cn(u,p,S){var k=u[p];(!(At.call(u,p)&&Fn(k,S))||S===void 0&&!(p in u))&&re(u,p,S)}function fe(u,p){for(var S=u.length;S--;)if(Fn(u[S][0],p))return S;return-1}function re(u,p,S){p=="__proto__"&&Vt?Vt(u,p,{configurable:!0,enumerable:!0,value:S,writable:!0}):u[p]=S}var oe=mc();function L(u){return u==null?u===void 0?G:w:Ht&&Ht in Object(u)?yc(u):Ac(u)}function h(u){return Ze(u)&&L(u)==s}function v(u){if(!de(u)||Sc(u))return!1;var p=gi(u)?je:mt;return p.test(Nc(u))}function M(u){return Ze(u)&&Ir(u.length)&&!!H[L(u)]}function Q(u){if(!de(u))return wc(u);var p=Nr(u),S=[];for(var k in u)k=="constructor"&&(p||!At.call(u,k))||S.push(k);return S}function V(u,p,S,k,Y){u!==p&&oe(p,function(ot,st){if(Y||(Y=new qt),de(ot))rt(u,p,st,S,V,k,Y);else{var et=k?k(hi(u,st),ot,st+"",u,p,Y):void 0;et===void 0&&(et=ot),Ye(u,st,et)}},Cr)}function rt(u,p,S,k,Y,ot,st){var et=hi(u,S),at=hi(p,S),Ft=st.get(at);if(Ft){Ye(u,S,Ft);return}var Rt=ot?ot(et,at,S+"",u,p,st):void 0,Qe=Rt===void 0;if(Qe){var mi=di(at),yi=!mi&&Rr(at),Or=!mi&&!yi&&Mr(at);Rt=at,mi||yi||Or?di(et)?Rt=et:Rc(et)?Rt=Dn(et):yi?(Qe=!1,Rt=Nt(at)):Or?(Qe=!1,Rt=ft(at)):Rt=[]:Ic(at)||fi(at)?(Rt=et,fi(et)?Rt=Mc(et):(!de(et)||gi(et))&&(Rt=_c(at))):Qe=!1}Qe&&(st.set(at,Rt),Y(Rt,at,k,ot,st),st.delete(at)),Ye(u,S,Rt)}function wt(u,p){return Ec(vc(u,p,Dr),u+"")}var Ct=Vt?function(u,p){return Vt(u,"toString",{configurable:!0,enumerable:!1,value:Dc(p),writable:!0})}:Dr;function Nt(u,p){return u.slice()}function qe(u){var p=new u.constructor(u.byteLength);return new vt(p).set(new vt(u)),p}function ft(u,p){var S=qe(u.buffer);return new u.constructor(S,u.byteOffset,u.length)}function Dn(u,p){var S=-1,k=u.length;for(p||(p=Array(k));++S<k;)p[S]=u[S];return p}function On(u,p,S,k){var Y=!S;S||(S={});for(var ot=-1,st=p.length;++ot<st;){var et=p[ot],at=void 0;at===void 0&&(at=u[et]),Y?re(S,et,at):Cn(S,et,at)}return S}function gc(u){return wt(function(p,S){var k=-1,Y=S.length,ot=Y>1?S[Y-1]:void 0,st=Y>2?S[2]:void 0;for(ot=u.length>3&&typeof ot=="function"?(Y--,ot):void 0,st&&Tc(S[0],S[1],st)&&(ot=Y<3?void 0:ot,Y=1),p=Object(p);++k<Y;){var et=S[k];et&&u(p,et,k,ot)}return p})}function mc(u){return function(p,S,k){for(var Y=-1,ot=Object(p),st=k(p),et=st.length;et--;){var at=st[++Y];if(S(ot[at],at,ot)===!1)break}return p}}function Ln(u,p){var S=u.__data__;return bc(p)?S[typeof p=="string"?"string":"hash"]:S.map}function ui(u,p){var S=Gt(u,p);return v(S)?S:void 0}function yc(u){var p=At.call(u,Ht),S=u[Ht];try{u[Ht]=void 0;var k=!0}catch{}var Y=We.call(u);return k&&(p?u[Ht]=S:delete u[Ht]),Y}function _c(u){return typeof u.constructor=="function"&&!Nr(u)?ri(we(u)):{}}function xr(u,p){var S=typeof u;return p=p??a,!!p&&(S=="number"||S!="symbol"&&it.test(u))&&u>-1&&u%1==0&&u<p}function Tc(u,p,S){if(!de(S))return!1;var k=typeof p;return(k=="number"?pi(S)&&xr(p,S.length):k=="string"&&p in S)?Fn(S[p],u):!1}function bc(u){var p=typeof u;return p=="string"||p=="number"||p=="symbol"||p=="boolean"?u!=="__proto__":u===null}function Sc(u){return!!He&&He in u}function Nr(u){var p=u&&u.constructor,S=typeof p=="function"&&p.prototype||ee;return u===S}function wc(u){var p=[];if(u!=null)for(var S in Object(u))p.push(S);return p}function Ac(u){return We.call(u)}function vc(u,p,S){return p=Et(p===void 0?u.length-1:p,0),function(){for(var k=arguments,Y=-1,ot=Et(k.length-p,0),st=Array(ot);++Y<ot;)st[Y]=k[p+Y];Y=-1;for(var et=Array(p+1);++Y<p;)et[Y]=k[Y];return et[p]=S(st),Be(u,this,et)}}function hi(u,p){if(!(p==="constructor"&&typeof u[p]=="function")&&p!="__proto__")return u[p]}var Ec=xc(Ct);function xc(u){var p=0,S=0;return function(){var k=ne(),Y=o-(k-S);if(S=k,Y>0){if(++p>=r)return arguments[0]}else p=0;return u.apply(void 0,arguments)}}function Nc(u){if(u!=null){try{return jt.call(u)}catch{}try{return u+""}catch{}}return""}function Fn(u,p){return u===p||u!==u&&p!==p}var fi=h((function(){return arguments})())?h:function(u){return Ze(u)&&At.call(u,"callee")&&!vn.call(u,"callee")},di=Array.isArray;function pi(u){return u!=null&&Ir(u.length)&&!gi(u)}function Rc(u){return Ze(u)&&pi(u)}var Rr=Ae||Oc;function gi(u){if(!de(u))return!1;var p=L(u);return p==m||p==A||p==l||p==R}function Ir(u){return typeof u=="number"&&u>-1&&u%1==0&&u<=a}function de(u){var p=typeof u;return u!=null&&(p=="object"||p=="function")}function Ze(u){return u!=null&&typeof u=="object"}function Ic(u){if(!Ze(u)||L(u)!=_)return!1;var p=we(u);if(p===null)return!0;var S=At.call(p,"constructor")&&p.constructor;return typeof S=="function"&&S instanceof S&&jt.call(S)==An}var Mr=Ge?wn(Ge):M;function Mc(u){return On(u,Cr(u))}function Cr(u){return pi(u)?Mn(u):Q(u)}var Cc=gc(function(u,p,S){V(u,p,S)});function Dc(u){return function(){return u}}function Dr(u){return u}function Oc(){return!1}e.exports=Cc})(Fe,Fe.exports)),Fe.exports}var Ka=ja(),hn=Wa(Ka);function Va(e){var t=0,n=e.children,i=n&&n.length;if(!i)t=1;else for(;--i>=0;)t+=n[i].value;e.value=t}function $a(){return this.eachAfter(Va)}function Xa(e,t){let n=-1;for(const i of this)e.call(t,i,++n,this);return this}function Ya(e,t){for(var n=this,i=[n],r,o,a=-1;n=i.pop();)if(e.call(t,n,++a,this),r=n.children)for(o=r.length-1;o>=0;--o)i.push(r[o]);return this}function qa(e,t){for(var n=this,i=[n],r=[],o,a,s,c=-1;n=i.pop();)if(r.push(n),o=n.children)for(a=0,s=o.length;a<s;++a)i.push(o[a]);for(;n=r.pop();)e.call(t,n,++c,this);return this}function Za(e,t){let n=-1;for(const i of this)if(e.call(t,i,++n,this))return i}function Qa(e){return this.eachAfter(function(t){for(var n=+e(t.data)||0,i=t.children,r=i&&i.length;--r>=0;)n+=i[r].value;t.value=n})}function Ja(e){return this.eachBefore(function(t){t.children&&t.children.sort(e)})}function tl(e){for(var t=this,n=el(t,e),i=[t];t!==n;)t=t.parent,i.push(t);for(var r=i.length;e!==n;)i.splice(r,0,e),e=e.parent;return i}function el(e,t){if(e===t)return e;var n=e.ancestors(),i=t.ancestors(),r=null;for(e=n.pop(),t=i.pop();e===t;)r=e,e=n.pop(),t=i.pop();return r}function nl(){for(var e=this,t=[e];e=e.parent;)t.push(e);return t}function il(){return Array.from(this)}function rl(){var e=[];return this.eachBefore(function(t){t.children||e.push(t)}),e}function ol(){var e=this,t=[];return e.each(function(n){n!==e&&t.push({source:n.parent,target:n})}),t}function*sl(){var e=this,t,n=[e],i,r,o;do for(t=n.reverse(),n=[];e=t.pop();)if(yield e,i=e.children)for(r=0,o=i.length;r<o;++r)n.push(i[r]);while(n.length)}function fn(e,t){e instanceof Map?(e=[void 0,e],t===void 0&&(t=cl)):t===void 0&&(t=ll);for(var n=new ke(e),i,r=[n],o,a,s,c;i=r.pop();)if((a=t(i.data))&&(c=(a=Array.from(a)).length))for(i.children=a,s=c-1;s>=0;--s)r.push(o=a[s]=new ke(a[s])),o.parent=i,o.depth=i.depth+1;return n.eachBefore(hl)}function al(){return fn(this).eachBefore(ul)}function ll(e){return e.children}function cl(e){return Array.isArray(e)?e[1]:null}function ul(e){e.data.value!==void 0&&(e.value=e.data.value),e.data=e.data.data}function hl(e){var t=0;do e.height=t;while((e=e.parent)&&e.height<++t)}function ke(e){this.data=e,this.depth=this.height=0,this.parent=null}ke.prototype=fn.prototype={constructor:ke,count:$a,each:Xa,eachAfter:qa,eachBefore:Ya,find:Za,sum:Qa,sort:Ja,path:tl,ancestors:nl,descendants:il,leaves:rl,links:ol,copy:al,[Symbol.iterator]:sl};function fl(e,t){return e.parent===t.parent?1:2}function Qn(e){var t=e.children;return t?t[0]:e.t}function Jn(e){var t=e.children;return t?t[t.length-1]:e.t}function dl(e,t,n){var i=n/(t.i-e.i);t.c-=i,t.s+=n,e.c+=i,t.z+=n,t.m+=n}function pl(e){for(var t=0,n=0,i=e.children,r=i.length,o;--r>=0;)o=i[r],o.z+=t,o.m+=t,t+=o.s+(n+=o.c)}function gl(e,t,n){return e.a.parent===t.parent?e.a:n}function dn(e,t){this._=e,this.parent=null,this.children=null,this.A=null,this.a=this,this.z=0,this.m=0,this.c=0,this.s=0,this.t=null,this.i=t}dn.prototype=Object.create(ke.prototype);function ml(e){for(var t=new dn(e,0),n,i=[t],r,o,a,s;n=i.pop();)if(o=n._.children)for(n.children=new Array(s=o.length),a=s-1;a>=0;--a)i.push(r=n.children[a]=new dn(o[a],a)),r.parent=n;return(t.parent=new dn(null,0)).children=[t],t}function yl(){var e=fl,t=1,n=1,i=null;function r(l){var f=ml(l);if(f.eachAfter(o),f.parent.m=-f.z,f.eachBefore(a),i)l.eachBefore(c);else{var T=l,d=l,m=l;l.eachBefore(function(_){_.x<T.x&&(T=_),_.x>d.x&&(d=_),_.depth>m.depth&&(m=_)});var A=T===d?1:e(T,d)/2,b=A-T.x,g=t/(d.x+A+b),w=n/(m.depth||1);l.eachBefore(function(_){_.x=(_.x+b)*g,_.y=_.depth*w})}return l}function o(l){var f=l.children,T=l.parent.children,d=l.i?T[l.i-1]:null;if(f){pl(l);var m=(f[0].z+f[f.length-1].z)/2;d?(l.z=d.z+e(l._,d._),l.m=l.z-m):l.z=m}else d&&(l.z=d.z+e(l._,d._));l.parent.A=s(l,d,l.parent.A||T[0])}function a(l){l._.x=l.z+l.parent.m,l.m+=l.parent.m}function s(l,f,T){if(f){for(var d=l,m=l,A=f,b=d.parent.children[0],g=d.m,w=m.m,_=A.m,R=b.m,y;A=Jn(A),d=Qn(d),A&&d;)b=Qn(b),m=Jn(m),m.a=l,y=A.z+_-d.z-g+e(A._,d._),y>0&&(dl(gl(A,l,T),l,y),g+=y,w+=y),_+=A.m,g+=d.m,R+=b.m,w+=m.m;A&&!Jn(m)&&(m.t=A,m.m+=_-w),d&&!Qn(b)&&(b.t=d,b.m+=g-R,T=l)}return T}function c(l){l.x*=t,l.y=l.depth*n}return r.separation=function(l){return arguments.length?(e=l,r):e},r.size=function(l){return arguments.length?(i=!1,t=+l[0],n=+l[1],r):i?null:[t,n]},r.nodeSize=function(l){return arguments.length?(i=!0,t=+l[0],n=+l[1],r):i?[t,n]:null},r}function lr(e,t){const n=new Set(t.map(i=>i.target.id));for(const i of e)if(!n.has(i.id))return i;return e[0]}const _l=1e6;function cr(e,t){var s;const n=new Map;for(const c of e)n.set(c.id,[]);for(const c of t)(s=n.get(c.from.id))==null||s.push(c.to);let i=0,r=!1,o=null,a=-1;for(const c of e){const l=new Set([c.id]),f=[c];for(;f.length>0&&!r;){const d=f.pop();for(const m of n.get(d.id)??[]){if(++i>_l){r=!0;break}l.has(m.id)||(l.add(m.id),f.push(m))}}const T=l.size-1;if(T>a&&(a=T,o=c),r)break}return r&&console.warn("Pivotick: reachability search hit its traversal cap, using the best root found so far."),o??e[0]}function Tl(e,t){return ur(e,t)}function ur(e,t){const n=new Map,i=new Map;for(const l of e)n.set(l.id,[]),i.set(l.id,0);for(const l of t)l.directed!==!1&&(n.get(l.from.id).push(l.to),i.set(l.to.id,(i.get(l.to.id)||0)+1));const r=[],o=e.filter(l=>i.get(l.id)===0);for(;o.length;){const l=o.shift();r.push(l);for(const f of n.get(l.id))i.set(f.id,i.get(f.id)-1),i.get(f.id)===0&&o.push(f)}if(r.length!==e.length)return console.warn("Pivotick: the graph has a cycle, so no shallowest root is defined — using the first node."),e[0];const a=new Map;for(let l=r.length-1;l>=0;l--){const f=r[l];let T=0;for(const d of n.get(f.id))T=Math.max(T,1+(a.get(d.id)??0));a.set(f.id,T)}let s=null,c=1/0;for(const l of e){const f=a.get(l.id);f<c&&(c=f,s=l)}return s??e[0]}function bl(e,t,n){var T;const i=new Map(e.map(d=>[d.id,d])),r=new Map(e.map(d=>[d.id,[]]));for(const d of t)!r.has(d.from.id)||!r.has(d.to.id)||(r.get(d.from.id).push(d.to.id),r.get(d.to.id).push(d.from.id));const o=d=>{const m=new Map([[d,0]]),A=new Map,b=[d];for(let g=0;g<b.length;g++){const w=b[g];for(const _ of r.get(w)??[])m.has(_)||(m.set(_,m.get(w)+1),A.set(_,w),b.push(_))}return{levels:m,parentOf:A,farthest:b[b.length-1]}},a=n!==void 0&&r.has(n)?n:(T=e[0])==null?void 0:T.id;if(a===void 0)return e[0];const s=o(a).farthest,{parentOf:c,farthest:l}=o(s),f=[];for(let d=l;d!==void 0;d=c.get(d))f.push(d);return i.get(f[Math.floor(f.length/2)])??e[0]}const Sl=24,wl=16,ti=1,hr=.1;function ei(e,t){return e?e.measured<=0?mn[1]:e.needed/e.measured*t:ti}function ni(e){if(!Number.isFinite(e))return ti;const t=Math.ceil(e/hr)*hr;return Math.min(mn[1],Math.max(ti,Math.round(t*10)/10))}function Al(e){const t=ei(e.level,e.current.levelSpacing);if(e.radial){const n=ei(e.sibling,e.current.levelSpacing);return{levelSpacing:ni(Math.max(t,n)),siblingSpacing:e.current.siblingSpacing}}return{levelSpacing:ni(t),siblingSpacing:ni(ei(e.sibling,e.current.siblingSpacing))}}function vl(e,t){return e+t+Sl}function El(e,t){return e+t+wl}const xl=20,fr="__pivotick_forest_root__",dr="__pivotick_tree_spacer__",pr=4096,Nl=5e4,gr=.5;let mr="";const ii={type:"tree",rootId:void 0,parentKey:void 0,depthKey:void 0,rootIdAlgorithmFinder:"MaxReachability",strength:.25,radial:!1,radialGap:750,spacing:"auto",levelSpacing:1,siblingSpacing:1,horizontal:!1};class ${constructor(t,n,i,r={}){E(this,"graph");E(this,"simulation");E(this,"simulationForces");E(this,"options");E(this,"originalForceStrength");E(this,"canvasBCR");E(this,"levels");E(this,"maxDepth",0);E(this,"autoSpacing");E(this,"parkedIds",new Set);E(this,"positionedNodesByID");this.graph=t,this.simulation=n,this.simulationForces=i,this.options=hn({},ii,r),this.originalForceStrength={link:this.simulationForces.link.strength(),charge:this.simulationForces.charge.strength(),gravity:this.simulationForces.gravity.strength()},this.autoSpacing=r.spacing==="auto"||r.spacing!=="manual"&&r.levelSpacing===void 0&&r.siblingSpacing===void 0,this.positionedNodesByID=new Map,this.levels=new Map,this.setSizes(),this.update(),this.registerForces()}update(){var n,i;if(this.layoutOnce(),!this.autoSpacing||this.positionedNodesByID.size===0)return;const t=Al(this.measureAutoContext());t.levelSpacing===this.options.levelSpacing&&t.siblingSpacing===this.options.siblingSpacing||(this.options.levelSpacing=t.levelSpacing,this.options.siblingSpacing=t.siblingSpacing,this.layoutOnce(),(i=(n=this.graph.UIManager)==null?void 0:n.physicsFlyout)==null||i.syncAutoSpacing(t))}layoutOnce(){const t=this.graph.getNodes(),n=this.graph.getEdges(),i=this.buildLevels(t,n,this.options),{levels:r,maxDepth:o,parked:a}=i;this.parkedIds=new Set(a);const{nodes:s,nodeById:c}=this.buildTree(t,n,this.options,this.canvasBCR,i);this.positionedNodesByID=c,this.levels=r,this.maxDepth=o,s&&this.setNodePositions(s,this.options)}measureAutoContext(){const t=new Map;for(const[o,a]of this.positionedNodesByID){const s=this.graph.getMutableNode(o);if(!s||this.parkedIds.has(o))continue;const c=s.expanded?s.getCircleRadiusCollapsed():s.getCircleRadius(),l=Number.isFinite(c)?c:0,f=t.get(a.depth)??[];f.push({node:a,radius:l}),t.set(a.depth,f)}const n=[...t.keys()].sort((o,a)=>o-a);let i=null,r=null;for(let o=0;o<n.length;o++){const a=t.get(n[o]),s=o+1<n.length?t.get(n[o+1]):void 0;if(s){const l=Math.max(n[o+1]-n[o],1),f=Math.abs((s[0].node.y??0)-(a[0].node.y??0))/l,T=vl($.widestOf(a),$.widestOf(s));i=$.tighter(i,{measured:f,needed:T})}const c=[...a].sort((l,f)=>(l.node.x??0)-(f.node.x??0));for(let l=1;l<c.length;l++){const[f,T]=[c[l-1],c[l]],d=this.options.radial?2*(T.node.y??0)*Math.sin(Math.abs((T.node.x??0)-(f.node.x??0))/2):(T.node.x??0)-(f.node.x??0),m=El(f.radius,T.radius);r=$.tighter(r,{measured:d,needed:m})}}return{level:i,sibling:r,radial:this.options.radial,current:this.getSpacing()}}static tighter(t,n){if(!t)return n;const i=r=>r.needed/Math.max(r.measured,1e-6);return i(n)>i(t)?n:t}static widestOf(t){return t.reduce((n,i)=>Math.max(n,i.radius),0)}setSizes(){const t=this.graph.renderer.getCanvas();if(!t)throw new Error("Canvas element is not defined in the graph renderer.");this.canvasBCR=t.getBoundingClientRect()}setNodePositions(t,n){for(const i of t){const r=this.graph.getMutableNode(i.data.id);if(r)if(n.radial){const o=i.x??0,a=i.y??0;r.x=a*Math.cos(o-Math.PI/2),r.y=a*Math.sin(o-Math.PI/2),r.fx=r.x,r.fy=r.y}else n.horizontal?(r.x=i.y,r.fx=i.y,r.y=i.x,delete r.fy):(r.x=i.x,r.y=i.y,r.fy=i.y,delete r.fx)}}unsetNodePositions(){this.graph.getMutableNodes().forEach(t=>{delete t.fy,delete t.fx})}registerForces(){const t=this.options.strength??.1;if(this.options.radial){const n=$.radialRingGap(this.options,this.maxDepth),i=Ni(r=>(this.levels.get(r.id)??1)*n,0,0).strength(t);this.simulation.force("tree-radial",i)}else this.simulation.force("tree-y",Ii(n=>{var i,r;return this.options.horizontal?((i=this.positionedNodesByID.get(n.id))==null?void 0:i.x)??0:((r=this.positionedNodesByID.get(n.id))==null?void 0:r.y)??0}).strength(t)),this.simulation.force("tree-x",Ri(n=>{var i,r;return this.options.horizontal?((i=this.positionedNodesByID.get(n.id))==null?void 0:i.y)??0:((r=this.positionedNodesByID.get(n.id))==null?void 0:r.x)??0}).strength(t));$.adjustOtherSimulationForces(this.simulationForces,this.options)}unregisterLayout(){this.unregisterForces(),this.unsetNodePositions()}unregisterForces(){this.simulation.force("tree-radial",null),this.simulation.force("tree-y",null),this.simulation.force("tree-x",null),$.resetOtherSimulationForces(this.simulationForces,this.originalForceStrength)}static registerForcesOnSimulation(t,n,i,r,o,a,s=this){const c=hn({},ii,o),l=c.strength??.1,f=a.width,T=a.height,d=[f/2,T/2],m=s.buildLevelsStatic(t,n,c),{levels:A,maxDepth:b}=m,{nodeById:g}=s.buildTreeStatic(t,n,c,a,m);if(c.radial){const w=s.radialRingGap(c,b),_=Ni(R=>(A.get(R.id)??1)*w,d[0],d[1]).strength(l);i.force("tree-radial",_)}else i.force("tree-y",Ii(w=>{var _,R;return c.horizontal?((_=g.get(w.id))==null?void 0:_.x)??0:((R=g.get(w.id))==null?void 0:R.y)??0}).strength(l)),i.force("tree-x",Ri(w=>{var _,R;return c.horizontal?((_=g.get(w.id))==null?void 0:_.y)??0:((R=g.get(w.id))==null?void 0:R.x)??0}).strength(l));s.adjustOtherSimulationForces(r,c)}static adjustOtherSimulationForces(t,n){n!=null&&n.radial?(t.link.strength(0),t.charge.strength(0),t.gravity.strength(0)):(t.link.strength(0),t.charge.strength(0),t.gravity.strength(1e-5))}static resetOtherSimulationForces(t,n){t.link.strength(n.link),t.charge.strength(n.charge),t.gravity.strength(n.gravity)}static simulationDone(t,n,i,r){const o=hn({},ii,r);for(const a of t)o.radial?(a.fx=a.x,a.fy=a.y):o.horizontal?(a.fx=a.x,delete a.fy):(a.fy=a.y,delete a.fx)}static radialRingGap(t,n){const i=t.radialGap*$.spacingOf(t).level;return n>0?i/n:i}static spacingOf(t){const n=i=>Number.isFinite(i)?i:1;return{level:n(t.levelSpacing),sibling:n(t.siblingSpacing)}}getSpacing(){const{level:t,sibling:n}=$.spacingOf(this.options);return{levelSpacing:t,siblingSpacing:n}}setSpacing(t){this.autoSpacing=!1,this.options.spacing="manual",t.levelSpacing!==void 0&&(this.options.levelSpacing=t.levelSpacing),t.siblingSpacing!==void 0&&(this.options.siblingSpacing=t.siblingSpacing),this.relayout()}isAutoSpacing(){return this.autoSpacing}getRoot(){return{rootId:this.options.rootId,algorithm:this.options.rootIdAlgorithmFinder}}setRoot(t){"rootId"in t?this.options.rootId=t.rootId:(this.options.rootId=void 0,this.options.rootIdAlgorithmFinder=t.algorithm),this.relayout()}enableAutoSpacing(){this.autoSpacing=!0,this.options.spacing="auto",this.relayout()}relayout(){this.setSizes(),this.update(),this.positionedNodesByID.size!==0&&this.registerForces()}static sizedTreeLayout(t,n){const i=$.spacingOf(t),r=yl();if(t.radial)return r.size([2*Math.PI,t.radialGap*i.level]),{treeLayout:r,offset:{x:0,y:0}};const o=t.horizontal?n.width:n.height,a=t.horizontal?n.height:n.width,s=a*i.sibling,c=o*i.level;return r.size([s,c]).separation((l,f)=>{var d,m;const T=((m=(d=l.parent)==null?void 0:d.children)==null?void 0:m.length)??1;return l.parent===f.parent?1.5/T:1.5}),{treeLayout:r,offset:{x:-(s-a)/2,y:-(c-o)/2}}}static packParked(t,n,i,r,o=new Map){if(!t.length)return[];const a=I=>{const B=I.getCircleRadius();return Number.isFinite(B)?B:0},s=2*t.reduce((I,B)=>Math.max(I,a(B)),0)+xl,c=(I,B,Z,ut)=>({data:I,depth:ut,x:B,y:Z,height:0}),l=n.map(I=>I.x??0),f=n.map(I=>I.y??0),T=t.filter(I=>o.has(I.id)),d=t.filter(I=>!o.has(I.id)),m=I=>o.get(I.id)??0,A=[...new Set(n.map(I=>I.depth))].sort((I,B)=>I-B),b=new Map;for(const I of n)b.set(I.depth,I.y??0);const g=A[0]??0,w=A[A.length-1]??0,_=w>g?((b.get(w)??0)-(b.get(g)??0))/(w-g):s,R=I=>b.get(I)??(A.length?(b.get(g)??0)+(I-g)*_:I*s);if(i.radial){const I=[],B=new Map;for(const it of T){const H=m(it);B.set(H,[...B.get(H)??[],it])}for(const[it,H]of B){const q=n.filter(U=>U.depth===it).map(U=>U.x??0).sort((U,St)=>U-St);let Qt=0,J=2*Math.PI;if(q.length){J=0;for(let U=0;U<q.length;U++){const St=U+1<q.length?q[U+1]:q[0]+2*Math.PI;St-q[U]>J&&(J=St-q[U],Qt=q[U])}}const Jt=J/(H.length+1);H.forEach((U,St)=>I.push(c(U,Qt+Jt*(St+1),R(it),it)))}const Z=new Set(f).size,ut=f.length?Math.max(...f):i.radialGap,mt=Z>0?ut/Z:ut;return d.forEach((it,H)=>I.push(c(it,H*2*Math.PI/d.length,ut+mt,Z+1))),I}if(!n.length){const I=Math.max(1,Math.floor(r.width/s)),B=[],Z=new Map;for(const mt of T){const it=m(mt),H=Z.get(it)??0;Z.set(it,H+1),B.push(c(mt,H*s,it*s,it))}const ut=Z.size?Math.max(...Z.keys())+1:0;return d.forEach((mt,it)=>{const H=ut+Math.floor(it/I);B.push(c(mt,it%I*s,H*s,H))}),B}const y=new Map,C=new Map;for(const I of n){const B=I.y??0,Z=I.x??0;y.set(B,Math.max(y.get(B)??Z,Z)),C.set(I.depth,Math.max(C.get(I.depth)??Z,Z))}const O=[...y.keys()].sort((I,B)=>I-B),G=Math.min(...l),P=Math.max(...l),F=O.length>1?O[1]-O[0]:s,z=[],X=[],nt=new Map;for(const I of T){const B=m(I),Z=R(B),ut=nt.get(Z)??0,mt=P-ut*s,it=C.get(B);if(it!==void 0&&mt-s<=it){X.push(I);continue}nt.set(Z,ut+1),z.push(c(I,mt,Z,B))}const N=[...d,...X];let D=0;for(const[I,B]of O.entries()){if(D>=N.length)break;const Z=nt.get(B)??0,ut=P-Z*s-((y.get(B)??P)+s),mt=Math.floor(ut/s);for(let it=0;it<mt&&D<N.length;it++)z.push(c(N[D++],P-(Z+it)*s,B,I))}const K=Math.max(1,Math.floor((P-G)/s)),j=O[O.length-1];for(let I=0;D<N.length;I++)z.push(c(N[D++],P-I%K*s,j+F*(1+Math.floor(I/K)),O.length));return z}static offsetTree(t,n){if(!(!n.x&&!n.y))for(const i of t)i.x=(i.x??0)+n.x,i.y=(i.y??0)+n.y}buildTree(t,n,i,r,o){return $.buildTreeStatic(t,n,i,r,o)}static isScaffolding(t){return t===fr||t.startsWith(dr)}static buildTreeStatic(t,n,i,r,o){if(!t.length)return{root:null,nodes:[],nodeById:new Map};const a=new Map;for(const F of t){const z=F;z.children=[],a.set(F.id,z)}const{parentOf:s,roots:c,parked:l,levels:f,declaredRows:T}=o??$.buildLevelsStatic(t,n,i);let d=0;const m=()=>({id:`${dr}${d++}`,children:[]}),A=F=>f.get(F)??0,b=(F,z,X,nt)=>{let N=F;for(let D=z+1;D<nt&&d<Nl;D++){const K=m();N.children.push(K),N=K}N.children.push(X)};for(const[F,z]of s){const X=a.get(F),nt=a.get(z);!X||!nt||(b(nt,A(z),X,A(F)),X.parent=nt)}const g=l.map(F=>a.get(F)).filter(F=>!!F),w=$.hierarchyRootFor(c,a,A,b,m);if(!w){if(!c.length&&g.length){const F=$.packParked(g,[],i,r,T);return{root:null,nodes:F,nodeById:new Map(F.map(z=>[z.data.id,z]))}}throw new Error(`Root node with id "${c[0]}" not found.`)}const{treeLayout:_,offset:R}=$.sizedTreeLayout(i,r),y=fn(w),C=_(y);$.offsetTree(C.descendants(),R);const O=C.descendants().filter(F=>!$.isScaffolding(F.data.id)),G=$.packParked(g,O,i,r,T),P=new Map;for(const F of G)P.set(F.data.id,F);return C.descendants().forEach(F=>{$.isScaffolding(F.data.id)||P.set(F.data.id,F)}),{root:C,nodes:[...O,...G],nodeById:P}}static hierarchyRootFor(t,n,i,r,o){if(t.length===1){const c=n.get(t[0]);if(!c||i(t[0])<=0)return c;const l=o();return r(l,0,c,i(t[0])),l}const a=t.map(c=>n.get(c)).filter(c=>!!c);if(!a.length)return;const s={id:fr,children:[]};for(const c of a)r(s,0,c,i(c.id));return s}buildLevels(t,n,i){return $.buildLevelsStatic(t,n,i)}static readDeclaredHierarchy(t,n,i){const r=new Map,o=new Map,a=new Map,s=d=>a.set(d,(a.get(d)??0)+1),c=i?n.parentKey:void 0,l=n.depthKey;if(!c&&!l)return{parentOf:r,rowOf:o,complaints:a};const f=new Set(t.map(d=>d.id));for(const d of t){const m=d.getData();if(l){const g=m[l];if(g!=null&&g!==""){const w=Math.floor(Number(g));Number.isFinite(w)&&w>=0&&w<=pr?o.set(d.id,w):s(`declared depths that are not a row between 0 and ${pr}`)}}if(!c)continue;const A=m[c];if(A==null||A==="")continue;const b=String(A);b===d.id?s("declared parents pointing at their own node"):f.has(b)?r.set(d.id,b):s("declared parents not in the layout")}const T=new Set;for(const d of[...r.keys()]){if(T.has(d))continue;const m=[],A=new Set;let b=d;for(;b!==void 0&&!T.has(b);){if(A.has(b)){r.delete(b),s("declared parent cycles broken");break}A.add(b),m.push(b),b=r.get(b)}for(const g of m)T.add(g)}return{parentOf:r,rowOf:o,complaints:a}}static warnAboutDeclared(t){if(!t.size)return;const n="[Pivotick] Tree layout ignored part of the declared hierarchy: "+[...t].map(([i,r])=>`${r} ${i}`).join(", ")+".";n!==mr&&(mr=n,console.warn(n))}static buildLevelsStatic(t,n,i={}){var nt;if(!t.length)return{levels:new Map,maxDepth:0,nodeCountPerLevel:{},parentOf:new Map,roots:[],parked:[],declaredRows:new Map};const r=i.rootId!==void 0&&t.some(N=>N.id===i.rootId)?i.rootId:void 0,o=$.readDeclaredHierarchy(t,i,r===void 0);let a=r!==void 0;const s=new Map,c=new Map(o.parentOf),l=new Map,f=new Set,T=new Set;for(const N of t)l.set(N.id,[]);for(const{source:N,target:D}of n)(nt=l.get(N.id))==null||nt.push(D.id),f.add(D.id),T.add(N.id),T.add(D.id);for(const[N,D]of o.parentOf)T.add(N),T.add(D),f.add(N);const d=()=>{var N;for(const{source:D,target:K}of n)(N=l.get(K.id))==null||N.push(D.id);a=!0};a&&d();const m=N=>T.has(N)||N===r,A=t.filter(N=>m(N.id)),b=t.filter(N=>!m(N.id)).map(N=>N.id),g=o.parentOf.size>0,w=(N,D)=>{const K=new Set;let j=D;for(;j!==void 0&&!K.has(j);){if(j===N)return!0;K.add(j),j=c.get(j)}return!1},_=new Set,R=N=>{if(_.has(N))return;_.add(N);const D=[N];let K=0;for(;K<D.length;){const j=D[K++];for(const I of l.get(j)??[])_.has(I)||(_.add(I),!c.has(I)&&!(g&&w(I,j))&&c.set(I,j),D.push(I))}},y=[];if(A.length){const N=g?A.filter(j=>!c.has(j.id)):A,D=N.length?N:A;let K=r??$.findRootId(D,n,i.rootIdAlgorithmFinder);if(r===void 0&&$.directedCoverage(K,l,n)<gr){const j=cr(D,n).id;$.directedCoverage(j,l,n)<gr&&(d(),K=bl(D,n,j).id)}R(K);for(const j of A){if(_.has(j.id))continue;const I=A.find(B=>!_.has(B.id)&&!f.has(B.id))??j;R(I.id)}c.has(K)||y.push(K);for(const j of A)j.id!==K&&!c.has(j.id)&&y.push(j.id)}const C=new Map;for(const[N,D]of c){const K=C.get(D)??[];K.push(N),C.set(D,K)}let O=0;const G=(N,D)=>{const K=[[N,D]];for(;K.length;){const[j,I]=K.pop();if(s.has(j))continue;const B=o.rowOf.get(j);B!==void 0&&B<I&&O++;const Z=B!==void 0&&B>I?B:I;s.set(j,Z);for(const ut of C.get(j)??[])K.push([ut,Z+1])}};for(const N of y)G(N,0);for(const N of A)s.has(N.id)||G(N.id,0);O&&o.complaints.set("declared depths clamped to just below their parent",O),$.warnAboutDeclared(o.complaints);const P=y.length>1?1:0;if(P)for(const[N,D]of s)s.set(N,D+P);let F=0;for(const N of s.values())N>F&&(F=N);if(b.length){const N=F+1;for(const D of b){const K=o.rowOf.get(D);s.set(D,K===void 0?N:K+P)}for(const D of s.values())D>F&&(F=D)}const z=new Map;for(const[N,D]of o.rowOf)z.set(N,D+P);const X={};for(const N of s.values())X[N]=(X[N]||0)+1;return{levels:s,maxDepth:F,nodeCountPerLevel:X,parentOf:c,roots:y,parked:b,declaredRows:z}}static directedCoverage(t,n,i){var s,c;const r=l=>{const f=new Set([t]),T=[t];for(let d=0;d<T.length;d++)for(const m of l.get(T[d])??[])f.has(m)||(f.add(m),T.push(m));return f.size},o=new Map;for(const l of n.keys())o.set(l,[]);for(const{source:l,target:f}of i)(s=o.get(l.id))==null||s.push(f.id),(c=o.get(f.id))==null||c.push(l.id);const a=r(o);return a===0?1:r(n)/a}static findRootId(t,n,i){switch(i){case"FirstZeroInDegree":return lr(t,n).id;case"MaxReachability":return cr(t,n).id;case"MinMaxDistance":return Tl(t,n).id;case"MinHeight":return ur(t,n).id;default:return lr(t,n).id}}}class le extends ${constructor(t,n,i,r){super(t,n,i,{...r,type:"tree"})}static registerForcesOnSimulation(t,n,i,r,o,a){$.registerForcesOnSimulation(t,n,i,r,o,a,le)}buildTree(t,n,i,r){return le.buildTreeStatic(t,n,i,r)}static buildTreeStatic(t,n,i,r){if(!t.length)return{root:null,nodes:[],nodeById:new Map};const o=new Map;for(const A of t){const b=A;b.children=[],o.set(A.id,b)}if(!i.rootId||!o.get(i.rootId))throw new Error("Ego Tree can only be created with a rootId");const a=i.rootId,s=o.get(a);if(s.children=[],!s)throw new Error(`Root node with id "${a}" not found.`);const c=new Set([s.id]);for(const A of n){const b=o.get(A.source.id),g=o.get(A.target.id);if(!b||!g)continue;const w=A.source.id===s.id?g:A.target.id===s.id?b:void 0;!w||c.has(w.id)||(c.add(w.id),s.children.push(w),w.parent=s)}const{treeLayout:l,offset:f}=le.sizedTreeLayout(i,r),T=fn(s),d=l(T);le.offsetTree(d.descendants(),f);const m=new Map;return d.descendants().forEach(A=>{m.set(A.data.id,A)}),{root:d,nodes:d.descendants(),nodeById:m}}}function Rl(e){var n;const t=(n=e.getData())==null?void 0:n.label;return typeof t=="string"?t:""}const yr=.3,Il=1,_r=4,Ml=400,Cl=24,Dl=6.5,Ol=10,Ll=140,Fl=1.15,kl=.35,Pl=.35,zl=.0058,Gl=38,Tr=95,Bl=300,Ul=.35,Hl=8,Wl=10,jl=.54,br=24,Kl=62,Vl=.2,$l=240,Xl=.9,pn=.001,Yl=.06,ql=.03,Zl=8,Sr=.002;function Se(e,t,n){return Math.max(t,Math.min(n,e))}function Pe(e){return Se(e,0,1)}function gn(e,t){const[n,i]=lt[t];return Se(e,n,i)}function Ql(e){return yr+(Il-yr)*wr(e)}function wr(e){return Pe(Math.log10(Math.max(e,1)/_r)/Math.log10(Ml/_r))}function Ar(e){const t=10+Math.sqrt(Math.max(0,e-10));return t*t/100}function Jl(e,t){const n=Math.pow(Math.max(1,t)/Wl,jl),i=Gl*Math.pow(Bl/Math.max(1,e),Ul);return Se(i*n,Hl,Tr)}function tc(e,t,n){const i=e/Ar(t);return Se(i/400*100,Jl(n,t),Tr)}function ec(e,t){return e/100*400*Ar(t)}function nc(e){const t=(e-.6)/1.7999999999999998;return gn(4+t*56,"collisionRadius")}function ic(e,t,n,i){const r=Math.max(1,Xl*.5*Math.min(t.width,t.height)),o=$l*n*e/(r*r*r),a=pn+(ql-pn)*Math.pow(1-wr(n),3),s=pn+(Yl-pn)*Pe(i),c=Math.max(a,s),l=Se(o,Sr,Math.max(Sr,c));return gn(100*Math.sqrt(Pe(l/Vl)),"centering")}function rc(e){return gn(1.2+.8*Math.log10(Math.max(e,1)),"settleTime")}function oc(e){const t=Pe(Math.log10(Math.max(e,1)/4)/Math.log10(125));return br+(Kl-br)*t}function sc(e){return{repulsion:Math.round(e.repulsion),linkDistance:Math.round(e.linkDistance),collisionRadius:Math.round(e.collisionRadius),friction:Math.round(e.friction),centering:Math.round(e.centering),settleTime:Math.round(e.settleTime*10)/10}}function ac(e){const t=Ql(e.nodeCount)*e.canvas.width*e.canvas.height;return{targetArea:t,spacing:Math.sqrt(t/Math.max(1,e.nodeCount))}}function lc(e){const{spacing:t}=ac(e),n=Math.max(1,e.radii.mean),i=Math.max(.8*t,Dl*n),r=2*n+Cl,o=Math.min(lt.linkDistance[1],Ol*n+Ll),a=gn(Se(i,r,Math.max(r,o)),"linkDistance"),s=zl*t*t,c=e.radii.totalArea/Math.max(1,e.nodeCount*a*a),l=Fl+kl*Pe(c/Pl),f=tc(s,n,e.nodeCount);return sc({repulsion:f,linkDistance:a,collisionRadius:nc(l),friction:oc(e.nodeCount),centering:ic(ec(f,n),e.canvas,e.nodeCount,e.looseNodeFraction),settleTime:rc(e.nodeCount)})}function cc(e,t){const n=new Map;for(const a of e)n.set(a,a);const i=a=>{let s=a;for(;n.get(s)!==s;)s=n.get(s);let c=a;for(;n.get(c)!==s;){const l=n.get(c);n.set(c,s),c=l}return s};let r=e.length;for(const[a,s]of t){if(!n.has(a)||!n.has(s))continue;const c=i(a),l=i(s);c!==l&&(n.set(c,l),r--)}let o=0;if(r>1){const a=new Map;for(const s of e){const c=i(s);a.set(c,(a.get(c)??0)+1)}for(const s of a.values())s<Zl&&(o+=s)}return{count:r,looseNodeFraction:e.length?o/e.length:0}}const ze={d3Alpha:1,d3AlphaMin:.001,d3AlphaDecay:.05,d3AlphaTarget:0,d3VelocityDecay:.45,d3LinkDistance:40,d3LinkStrength:null,d3ManyBodyStrength:-150,d3ManyBodyTheta:.9,d3CollideRadius:12,d3CollideRadiusMultiplier:1.2,d3CollideStrength:1,d3CollideIterations:1,d3GravityStrength:.1,d3GravityStrengthConnected:.001,enabled:!0,cooldownTime:2e3,useWorker:!0,warmupTicks:"auto",freezeNodesOnDrag:!0,gridSnappingEnabled:!1,gridSize:50,fitViewOnExpandCollapse:!1,layout:{type:"force"},callbacks:{onInit:()=>{},onStart:()=>{},onStop:()=>{},onTick:()=>{}}},lt={repulsion:[0,100],linkDistance:[40,600],collisionRadius:[4,60],friction:[0,100],centering:[0,100],settleTime:[.5,8]},mn=[.5,10],uc="MaxReachability",hc={levelSpacing:1,siblingSpacing:1},fc={tight:{repulsion:32,linkDistance:70,collisionRadius:16,friction:45,centering:7,settleTime:3},loose:{repulsion:70,linkDistance:150,collisionRadius:26,friction:28,centering:7,settleTime:2.25}},x=class x{constructor(t,n={}){E(this,"simulation");E(this,"graph");E(this,"container");E(this,"graphInteraction");E(this,"layout");E(this,"containerBCR");E(this,"containerObserver");E(this,"animationFrameId",null);E(this,"startSimulationTime",0);E(this,"engineRunning",!1);E(this,"slowTickThresholdReached",!1);E(this,"avgTickDuration",0);E(this,"SLOW_TICK_THRESHOLD",33);E(this,"dragInProgress",!1);E(this,"dragSelection",[]);E(this,"totalTickCount",0);E(this,"runTickCount",0);E(this,"options");E(this,"callbacks");E(this,"simulationForces");E(this,"scaledForces",{d3ManyBodyStrength:ze.d3ManyBodyStrength,d3CollideStrength:ze.d3CollideStrength});E(this,"physicsKnobs");E(this,"autoEnabled");E(this,"autoTuneTimer",null);E(this,"applyingAutoKnobs",!1);E(this,"suppressReheat",!1);E(this,"autoLastRun",null);if(this.graph=t,this.autoEnabled=x.shouldAutoTune(n),this.options=hn({},ze,n),this.callbacks=this.options.callbacks??{},this.physicsKnobs=x.knobsFromOptions(this.options),this.container=this.graph.renderer.getRootContainer(),!this.container)throw new Error("Root container is not defined in the graph renderer.");if(this.containerBCR=x.measureContainer(this.container),this.graphInteraction=this.graph.renderer.getGraphInteraction(),!this.graphInteraction)throw new Error("Graph interaction is not available.");const i=x.initSimulationForces(this.options,this.containerBCR);this.simulation=i.simulation,this.simulationForces=i.simulationForces,this.scaledForces.d3ManyBodyStrength=this.options.d3ManyBodyStrength||ze.d3ManyBodyStrength,this.scaledForces.d3CollideStrength=this.options.d3CollideStrength||ze.d3CollideStrength,this.options.layout.type==="tree"?this.layout=new $(this.graph,this.simulation,this.simulationForces,this.options.layout):this.options.layout.type==="egoTree"&&(this.layout=new le(this.graph,this.simulation,this.simulationForces,this.options.layout)),this.layout&&Object.assign(this.options.layout,this.layout.getSpacing()),this.observeContainer(),this.callbacks.onInit&&this.callbacks.onInit(this)}static measureContainer(t){const n=t.getBoundingClientRect();if(n.width>0&&n.height>0)return n;const{width:i,height:r}=x.FALLBACK_CONTAINER_SIZE;return new DOMRect(n.x,n.y,i,r)}observeContainer(){!this.container||typeof ResizeObserver>"u"||(this.containerObserver=new ResizeObserver(()=>{if(!this.container)return;const t=x.measureContainer(this.container);t.width===this.containerBCR.width&&t.height===this.containerBCR.height||(this.containerBCR=t,x.initSimulationForceGravity(this.simulationForces.gravity,this.options,t),this.scheduleTune())}),this.containerObserver.observe(this.container))}destroy(){var t;this.stop(),(t=this.containerObserver)==null||t.disconnect(),this.containerObserver=void 0,this.container=void 0}static initSimulationForces(t,n){const i={link:Jr(),charge:mo(),collide:Zr(),gravity:yo()},r=go().force("link",i.link).force("charge",i.charge).force("collide",i.collide).force("gravity",i.gravity);return this.initSimulationForceGravity(i.gravity,t,n),this.initSimulationForceLink(i.link,t),this.initSimulationForceCharge(i.charge,t),this.initSimulationForceCollide(i.collide,t),r.alphaMin(t.d3AlphaMin),r.alphaDecay(t.d3AlphaDecay),r.alphaTarget(0),r.velocityDecay(t.d3VelocityDecay),{simulation:r,simulationForces:i}}static initSimulationForceGravity(t,n,i){t.x(i.width/2).y(i.height/2).strength(r=>(r.degree()??0)===0?n.d3GravityStrength:n.d3GravityStrengthConnected)}static initSimulationForceLink(t,n){t.distance(i=>{const r=i.__clusterAnchorDistance;if(r!=null)return r;const o=Rl(i);if(!o||o==="")return n.d3LinkDistance;const a=o.length*10;return Math.max(n.d3LinkDistance,a)}),n.d3LinkStrength&&t.strength(n.d3LinkStrength)}static initSimulationForceCharge(t,n){t.theta(n.d3ManyBodyTheta).strength(i=>{const r=i,o=n.d3ManyBodyStrength,a=r.expanded?r.getCircleRadiusCollapsed():r.getCircleRadius(),s=10+Math.sqrt(Math.max(0,a-10));let c=r.weight??1;return c*=r.isParent?10:1,o*(s*s)/100*c})}static initSimulationForceCollide(t,n){const i=n.d3CollideRadiusMultiplier;t.radius(r=>{const o=r;return o.expanded?i*o.getCircleRadius()+20:o.getCircleRadius()?i*o.getCircleRadius():n.d3CollideRadius}).strength(n.d3CollideStrength)}static initSimulationForceClusterRadialConstraint(t,n){t.strength(n.d3CollideStrength)}update(){this.layout?(this.layout.update(),Object.assign(this.options.layout,this.layout.getSpacing())):this.scheduleTune();const t=this.graph.getMutableNodes().filter(i=>i.visible);this.simulation.nodes(t);const n=this.simulation.force("link");n&&n.id(i=>i.id).links(this.getActiveEdges()),this.restart()}getActiveEdges(){const t=new Set(this.graph.getMutableNodes().filter(a=>a.visible).map(a=>a.id)),n=a=>{let s=a;for(;s&&!t.has(s.id);)s=s.parentNode;return s},i=(a,s)=>a<s?`${a}|${s}`:`${s}|${a}`,r=[],o=new Set;for(const a of this.graph.getMutableEdges()){if(!a.visibleIgnoringLayer)continue;const s=a.source,c=a.target;if(!s.isChild&&!c.isChild){r.push(a),o.add(i(s.id,c.id));continue}if(s.isChild&&c.isChild)continue;const l=s.isChild?c:s,f=n(s.isChild?s:c);if(!f||f.id===l.id)continue;const T=i(l.id,f.id);o.has(T)||(o.add(T),r.push(this.clusterAnchorLink(l,f)))}return r}clusterAnchorLink(t,n){return{id:`cluster-anchor-${t.id}-${n.id}`,source:t,target:n,__clusterAnchorDistance:n.getCircleRadius()+this.options.d3LinkDistance}}enable(){this.avgTickDuration=0,this.options.enabled=!0,this.start(!1)}disable(){this.options.enabled=!1,this.stop()}pause(){this.engineRunning=!1,this.slowTickThresholdReached=!1}restart(){this.startSimulationTime=new Date().getTime(),this.runTickCount=0,this.engineRunning=!0,this.slowTickThresholdReached=!1}async start(t=!0){if(t&&(this.tuneNow({reheat:!1}),await this.runSimulationWorkerRouter()),!this.options.enabled){this.engineRunning=!1;return}this.engineRunning=!0,this.slowTickThresholdReached=!1,this.callbacks.onStart&&this.callbacks.onStart(this),this.animationFrameId===null&&this.startAnimationLoop()}stop(){this.engineRunning=!1,this.autoTuneTimer!==null&&(clearTimeout(this.autoTuneTimer),this.autoTuneTimer=null),this.animationFrameId!==null&&(cancelAnimationFrame(this.animationFrameId),this.animationFrameId=null),this.simulation.stop(),this.callbacks.onStop&&this.callbacks.onStop(this)}startAnimationLoop(){const t=()=>{this.animationFrameId=requestAnimationFrame(t),this.simulationTick()};this.engineRunning=!0,this.simulation.alpha(.01).restart(),this.animationFrameId=requestAnimationFrame(t)}simulationTick(){if(this.engineRunning){!this.dragInProgress&&this.cooledDown()&&(this.engineRunning=!1,this.simulation.stop(),this.callbacks.onStop&&this.callbacks.onStop(this)),this.totalTickCount++,this.runTickCount++;const t=performance.now();this.simulation.tick(),this.graph.nextTick(),this.updateTickMetrics(performance.now()-t),this.callbacks.onTick&&this.callbacks.onTick(this),this.graphInteraction.simulationTick(),this.totalTickCount%10===0&&this.graphInteraction.simulationSlowTick()}}cooledDown(){const t=this.options.cooldownTime/1e3*x.NOMINAL_FPS;return this.runTickCount>=t||this.options.d3AlphaMin>0&&this.simulation.alpha()<this.options.d3AlphaMin?!0:new Date().getTime()-this.startSimulationTime>this.options.cooldownTime*x.COOLDOWN_WALL_GRACE}updateTickMetrics(t){var n;this.avgTickDuration=this.avgTickDuration*.9+t*.1,this.avgTickDuration>this.SLOW_TICK_THRESHOLD&&(this.slowTickThresholdReached=!0,this.disable(),this.graph.UIManager.showNotification({level:"warning",title:"Physics engine running slow",message:"The physic has been disabled."}),(n=this.graph.UIManager.physicsFlyout)==null||n.syncRunState())}async waitForSimulationStop(){if(this.engineRunning)return new Promise(t=>{const n=this.callbacks.onStop;this.callbacks.onStop=i=>{n==null||n(i),this.callbacks.onStop=n,t()}})}isEnabled(){return this.options.enabled}applyComputedPositions(t){const n=new Map(t.map(i=>[i.id,i]));for(const i of this.graph.getMutableNodes()){const r=n.get(i.id);r&&(i.x=r.x,i.y=r.y,i.fx=typeof r.fx=="number"?r.fx:void 0,i.fy=typeof r.fy=="number"?r.fy:void 0)}}async computeGraph(t={}){const{runSimulation:n}=await Promise.resolve().then(function(){return pc}),i=this.containerBCR,r=this.graph.getMutableNodes(),o=this.graph.getNodes(),a=this.graph.getEdges(),{callbacks:s,...c}=this.options;Object.assign(c,t);const{nodes:l}=n(o,a,c,i);this.applyComputedPositions(l),this.graph.updateData(r,void 0,!1)}async runSimulationWorkerRouter(t={}){if(this.options.useWorker)try{await this.runSimulationWorker(t);return}catch(n){this.options.useWorker=!1,console.warn("[Pivotick] Simulation Web Worker unavailable (often a CSP blocking blob workers); falling back to the main thread. Set `simulation.useWorker: false` to silence this.",n)}await this.computeGraph(t),this.graph.updateLayoutProgress(100,0,"done")}async runSimulationWorker(t={}){const n=this.containerBCR,i=this.graph.getMutableNodes(),r=this.graph.getNodes().map(f=>f.toSimulationDTO()),o=this.graph.getEdges().map(f=>f.toSimulationDTO()),a=(f,T)=>{this.graph.updateLayoutProgress(f,T,"simulation")},{callbacks:s,...c}=this.options;Object.assign(c,t);const{nodes:l}=await Ha(r,o,c,n,a);this.graph.updateLayoutProgress(100,0,"rendering"),this.applyComputedPositions(l),this.graph.updateData(i,void 0,!1),this.graph.updateLayoutProgress(100,0,"done")}reheat(t=.7){this.restart(),this.simulation.alpha(t).restart()}refreshForcesAndReheat(t=.5){if(!this.options.enabled)return;this.tuneNow({reheat:!1});const n=this.graph.getMutableNodes().filter(i=>i.visible);this.simulation.nodes(n),this.reheat(t)}setRepulsion(t){const n=x.clamp(t,lt.repulsion);this.physicsKnobs.repulsion=n,this.options.d3ManyBodyStrength=x.mapLinear(n,lt.repulsion,x.REPULSION_STRENGTH_RANGE),this.scaledForces.d3ManyBodyStrength=this.options.d3ManyBodyStrength,x.initSimulationForceCharge(this.simulationForces.charge,this.options),this.noteManualKnobEdit(),this.reheatIfEnabled()}setLinkDistance(t){const n=x.clamp(t,lt.linkDistance);this.physicsKnobs.linkDistance=n,this.options.d3LinkDistance=x.mapLinear(n,lt.linkDistance,x.LINK_DISTANCE_RANGE),x.initSimulationForceLink(this.simulationForces.link,this.options),this.noteManualKnobEdit(),this.reheatIfEnabled()}setCollisionRadius(t){const n=x.clamp(t,lt.collisionRadius);this.physicsKnobs.collisionRadius=n,this.options.d3CollideRadiusMultiplier=x.mapLinear(n,lt.collisionRadius,x.COLLIDE_MULTIPLIER_RANGE),x.initSimulationForceCollide(this.simulationForces.collide,this.options),this.noteManualKnobEdit(),this.reheatIfEnabled()}setFriction(t){const n=x.clamp(t,lt.friction);this.physicsKnobs.friction=n,this.options.d3VelocityDecay=x.mapLinear(n,lt.friction,x.FRICTION_DECAY_RANGE),this.simulation.velocityDecay(this.options.d3VelocityDecay),this.noteManualKnobEdit()}setCentering(t){const n=x.clamp(t,lt.centering);this.physicsKnobs.centering=n,this.options.d3GravityStrengthConnected=x.gravityForCentering(n),this.options.d3GravityStrength=x.isolatedGravityFor(this.options.d3GravityStrengthConnected),x.initSimulationForceGravity(this.simulationForces.gravity,this.options,this.containerBCR),this.noteManualKnobEdit(),this.reheatIfEnabled()}setSettleTime(t){const n=x.clamp(t,lt.settleTime);this.physicsKnobs.settleTime=n,this.options.d3AlphaDecay=x.alphaDecayForSettleTime(n,this.options.d3AlphaMin),this.options.cooldownTime=n*1e3,this.simulation.alphaDecay(this.options.d3AlphaDecay),this.noteManualKnobEdit()}applyPhysicsPreset(t){this.disableAutoPhysics(),this.writeKnobs(fc[t]),this.reheatIfEnabled(x.CLICK_REHEAT_ALPHA)}writeKnobs(t){this.physicsKnobs={...t},this.options.d3ManyBodyStrength=x.mapLinear(t.repulsion,lt.repulsion,x.REPULSION_STRENGTH_RANGE),this.scaledForces.d3ManyBodyStrength=this.options.d3ManyBodyStrength,this.options.d3LinkDistance=x.mapLinear(t.linkDistance,lt.linkDistance,x.LINK_DISTANCE_RANGE),this.options.d3CollideRadiusMultiplier=x.mapLinear(t.collisionRadius,lt.collisionRadius,x.COLLIDE_MULTIPLIER_RANGE),this.options.d3VelocityDecay=x.mapLinear(t.friction,lt.friction,x.FRICTION_DECAY_RANGE),this.options.d3GravityStrengthConnected=x.gravityForCentering(t.centering),this.options.d3GravityStrength=x.isolatedGravityFor(this.options.d3GravityStrengthConnected),this.options.d3AlphaDecay=x.alphaDecayForSettleTime(t.settleTime,this.options.d3AlphaMin),this.options.cooldownTime=t.settleTime*1e3,x.initSimulationForceCharge(this.simulationForces.charge,this.options),x.initSimulationForceLink(this.simulationForces.link,this.options),x.initSimulationForceCollide(this.simulationForces.collide,this.options),x.initSimulationForceGravity(this.simulationForces.gravity,this.options,this.containerBCR),this.simulation.velocityDecay(this.options.d3VelocityDecay),this.simulation.alphaDecay(this.options.d3AlphaDecay)}getPhysicsKnobs(){return{...this.physicsKnobs}}getLayoutType(){return this.options.layout.type}getTreeSpacing(){var t;return((t=this.layout)==null?void 0:t.getSpacing())??{...hc}}setTreeSpacing(t){if(!this.layout)return;const n={};t.levelSpacing!==void 0&&(n.levelSpacing=x.clamp(t.levelSpacing,mn)),t.siblingSpacing!==void 0&&(n.siblingSpacing=x.clamp(t.siblingSpacing,mn)),this.layout.setSpacing(n),Object.assign(this.options.layout,n,{spacing:"manual"}),this.graph.nextTick(),this.reheatIfEnabled()}getTreeRoot(){var t;return((t=this.layout)==null?void 0:t.getRoot())??{algorithm:uc}}setTreeRoot(t){if(!this.layout)return;this.layout.setRoot(t);const n=this.layout.getRoot();Object.assign(this.options.layout,{rootId:n.rootId,rootIdAlgorithmFinder:n.algorithm}),this.graph.nextTick(),this.reheatIfEnabled()}isAutoTreeSpacingEnabled(){var t;return((t=this.layout)==null?void 0:t.isAutoSpacing())??!1}enableAutoTreeSpacing(){this.layout&&(this.layout.enableAutoSpacing(),Object.assign(this.options.layout,{spacing:"auto"}),this.graph.nextTick(),this.reheatIfEnabled())}reheatIfEnabled(t=.5){this.suppressReheat||this.options.enabled&&this.reheat(t)}static clamp(t,[n,i]){return Math.max(n,Math.min(i,t))}static mapLinear(t,n,i){const r=(t-n[0])/(n[1]-n[0]);return i[0]+r*(i[1]-i[0])}static knobsFromOptions(t){const n=(r,o,a)=>Math.round(x.clamp(x.mapLinear(r,o,lt[a]),lt[a])),i=x.settleTimeFromAlphaDecay(t.d3AlphaDecay,t.d3AlphaMin);return{repulsion:n(t.d3ManyBodyStrength,x.REPULSION_STRENGTH_RANGE,"repulsion"),linkDistance:n(t.d3LinkDistance,x.LINK_DISTANCE_RANGE,"linkDistance"),collisionRadius:n(t.d3CollideRadiusMultiplier,x.COLLIDE_MULTIPLIER_RANGE,"collisionRadius"),friction:n(t.d3VelocityDecay,x.FRICTION_DECAY_RANGE,"friction"),centering:Math.round(x.clamp(x.centeringFromGravity(t.d3GravityStrengthConnected),lt.centering)),settleTime:Math.round(x.clamp(i,lt.settleTime)*10)/10}}static gravityForCentering(t){const n=t/lt.centering[1];return x.CENTERING_STRENGTH_MAX*n*n}static centeringFromGravity(t){const n=Math.sqrt(Math.max(0,t)/x.CENTERING_STRENGTH_MAX);return lt.centering[1]*n}static isolatedGravityFor(t){const[n,i]=x.CENTERING_ISOLATED_RANGE;return Math.max(n,Math.min(i,t*x.CENTERING_ISOLATED_MULTIPLE))}static alphaDecayForSettleTime(t,n){const i=Math.max(1,t*x.NOMINAL_FPS),r=Math.min(.999,Math.max(1e-6,n));return 1-Math.pow(r,1/i)}static settleTimeFromAlphaDecay(t,n){const i=Math.min(.999,Math.max(1e-6,n)),r=Math.min(.999,Math.max(1e-6,t));return Math.log(i)/Math.log(1-r)/x.NOMINAL_FPS}static shouldAutoTune(t){return t.physics==="auto"?!0:t.physics==="manual"?!1:!x.AUTO_OWNED_OPTIONS.some(n=>t[n]!==void 0)}isAutoPhysicsEnabled(){return this.autoEnabled}enableAutoPhysics(){this.autoEnabled=!0,this.tuneNow({alpha:x.CLICK_REHEAT_ALPHA,force:!0})}disableAutoPhysics(){this.autoEnabled=!1,this.autoTuneTimer!==null&&(clearTimeout(this.autoTuneTimer),this.autoTuneTimer=null)}getAutoRun(){return this.autoLastRun}noteManualKnobEdit(){this.applyingAutoKnobs||this.disableAutoPhysics()}scheduleTune(){this.autoEnabled&&(this.autoTuneTimer!==null&&clearTimeout(this.autoTuneTimer),this.autoTuneTimer=setTimeout(()=>{this.autoTuneTimer=null,this.tuneNow()},x.AUTO_DEBOUNCE_MS))}tuneNow(t={}){var l;const{reheat:n=!0,alpha:i=x.AUTO_REHEAT_ALPHA,force:r=!1}=t;if(!this.autoEnabled||this.options.layout.type!=="force")return;const o=this.buildAutoContext();if(o.nodeCount===0)return;const a=lc(o),c=Object.keys(a).every(f=>{const[T,d]=lt[f];return Math.abs(a[f]-this.physicsKnobs[f])<=(d-T)*x.AUTO_DEADBAND})&&!r;if(this.autoLastRun={context:o,knobs:c?this.getPhysicsKnobs():a,skipped:c},!c){this.applyingAutoKnobs=!0,this.suppressReheat=!0;try{this.writeKnobs(a)}finally{this.suppressReheat=!1,this.applyingAutoKnobs=!1}n&&this.reheatIfEnabled(i),(l=this.graph.UIManager.physicsFlyout)==null||l.syncAutoKnobs(this.getPhysicsKnobs())}}buildAutoContext(){const t=this.containerBCR,n=this.graph.getMutableNodes().filter(c=>c.visible),i=this.getActiveEdges();let r=0,o=0,a=0;for(const c of n){const l=c.expanded?c.getCircleRadiusCollapsed():c.getCircleRadius();r+=l,o=Math.max(o,l),a+=Math.PI*l*l}const s=cc(n.map(c=>c.id),i.map(c=>[c.source.id,c.target.id]));return{canvas:{width:t.width,height:t.height},nodeCount:n.length,radii:{mean:n.length?r/n.length:0,max:o,totalArea:a},edgeCount:i.length,componentCount:s.count,looseNodeFraction:s.looseNodeFraction,current:this.getPhysicsKnobs()}}createDragBehavior(){return la().filter(()=>!this.graph.editing.connectManager.isActiveAndNotIdle()).on("start.draggedelement",(t,n)=>{this.graphInteraction.hasActiveMultiselection()?this.dragSelection=this.graphInteraction.getSelectedNodes().map(i=>{const{node:r}=i;return r.freeze(),{node:r,dx:r.x-n.x,dy:r.y-n.y}}):(this.dragSelection=[],n.freeze())}).on("drag.draggedelement",(t,n)=>{if(!this.dragInProgress&&this.isEnabled()&&(this.dragInProgress=!0,this.restart(),this.simulation.alphaTarget(.3).restart()),this.graphInteraction.hasActiveMultiselection())this.dragSelection.forEach(({node:i,dx:r,dy:o})=>{const a=this.applySnap(t.x+r),s=this.applySnap(t.y+o);i.fx=a,i.fy=s,i.x=a,i.y=s});else{const i=this.applySnap(t.x),r=this.applySnap(t.y);n.fx=i,n.fy=r,n.x=i,n.y=r}if(this.graphInteraction.dragging(t.sourceEvent,t.subject),!this.engineRunning||!this.isEnabled()){const i=this.graphInteraction.hasActiveMultiselection()?this.dragSelection.map(r=>r.node):[n];this.graph.nextTickFor(i)}}).on("end.draggedelement",(t,n)=>{!t.active&&this.dragInProgress&&(this.dragInProgress=!1,this.restart(),this.simulation.alphaTarget(this.options.d3AlphaTarget).restart()),this.options.freezeNodesOnDrag||(this.graphInteraction.hasActiveMultiselection()?(this.dragSelection.forEach(({node:i})=>i.unfreeze()),this.dragSelection=[]):n.unfreeze()),this.graphInteraction.dragended(t.sourceEvent,t.subject)})}isDragging(){return this.dragInProgress}toggleGridSnapping(){this.options.gridSnappingEnabled=!this.options.gridSnappingEnabled}toggleFreezeNodesOnDrag(){this.options.freezeNodesOnDrag=!this.options.freezeNodesOnDrag}isFreezeNodesOnDrag(){return this.options.freezeNodesOnDrag}isGridSnappingEnabled(){return this.options.gridSnappingEnabled}toggleFitViewOnExpandCollapse(){this.options.fitViewOnExpandCollapse=!this.options.fitViewOnExpandCollapse}isFitViewOnExpandCollapse(){return this.options.fitViewOnExpandCollapse}applySnap(t){return this.options.gridSnappingEnabled?Math.round(t/this.options.gridSize)*this.options.gridSize:t}snapToGrid(t){return this.applySnap(t)}getForceSimulation(){return this.simulationForces}getSimulation(){return this.simulation}async changeLayout(t,n={}){var i;this.layout&&((i=this.layout)==null||i.unregisterLayout(),this.layout=void 0),n=n??{},n.layout=n.layout??{},n.layout.type=t,t==="force"?(x.initSimulationForceCharge(this.simulationForces.charge,this.options),x.initSimulationForceCollide(this.simulationForces.collide,this.options)):t==="tree"&&(this.layout=new $(this.graph,this.simulation,this.simulationForces,n.layout)),this.options.layout.type=t,this.update(),this.pause(),await this.runSimulationWorkerRouter(n),this.restart(),await this.waitForSimulationStop(),this.graph.renderer.fitAndCenterWhenSettled()}};E(x,"REPULSION_STRENGTH_RANGE",[0,-400]),E(x,"LINK_DISTANCE_RANGE",[40,600]),E(x,"COLLIDE_MULTIPLIER_RANGE",[.6,2.4]),E(x,"FRICTION_DECAY_RANGE",[0,1]),E(x,"CENTERING_STRENGTH_MAX",.2),E(x,"CENTERING_ISOLATED_MULTIPLE",4),E(x,"CENTERING_ISOLATED_RANGE",[.1,.3]),E(x,"AUTO_OWNED_OPTIONS",["d3LinkDistance","d3ManyBodyStrength","d3CollideRadiusMultiplier","d3VelocityDecay","d3GravityStrength","d3GravityStrengthConnected","d3AlphaDecay","cooldownTime"]),E(x,"AUTO_DEBOUNCE_MS",150),E(x,"AUTO_DEADBAND",.04),E(x,"AUTO_REHEAT_ALPHA",.3),E(x,"CLICK_REHEAT_ALPHA",1),E(x,"NOMINAL_FPS",60),E(x,"COOLDOWN_WALL_GRACE",4),E(x,"FALLBACK_CONTAINER_SIZE",{width:1e3,height:800});let yn=x;const vr=1e4,_n=2e4,Tn=.15*_n;self.onmessage=e=>{var b,g,w;if(e.data.source!=="simulation-worker-wrapper")return;const{nodes:t,edges:n,options:i,canvasBCR:r}=e.data,o=t.map(_=>{const R=new sr(_.id,_.data,_.style);return R.setCircleRadius(_._circleRadius??10),typeof _.x=="number"&&(R.x=_.x),typeof _.y=="number"&&(R.y=_.y),typeof _.fx=="number"&&(R.fx=_.fx),typeof _.fy=="number"&&(R.fy=_.fy),R}),a=new Map(o.map(_=>[_.id,_])),{simulation:s,simulationForces:c}=yn.initSimulationForces(i,r),l=[];for(const _ of n){const R=a.get(_.from.id),y=a.get(_.to.id);if(R&&y){const C=_.style??{};l.push(new cn(_.id,R,y,_.data,C,_.directed))}}s.nodes(o);const f=s.force("link");f&&f.id(_=>_.id).links(l),((b=i.layout)==null?void 0:b.type)==="tree"?$.registerForcesOnSimulation(o,l,s,c,i.layout,r,$):((g=i.layout)==null?void 0:g.type)==="egoTree"&&$.registerForcesOnSimulation(o,l,s,c,i.layout,r,le);let T=i.warmupTicks||_n;T=T==="auto"?_n:T,T=T-Tn;let d=.3;s.alphaTarget(d);const m=new Date().getTime();let A;for(let _=0;_<T&&!(new Date().getTime()-m>vr||new Date().getTime()-m>i.cooldownTime||bn(i,s,d)&&new Date().getTime()-m>i.cooldownTime*.15);++_)_%5===0&&(A=Er(_,new Date().getTime()-m,i),postMessage({type:"tick",progress:A,elapsedTime:new Date().getTime()-m})),s.tick();d=0,s.alphaTarget(d),s.alpha(1);for(let _=0;_<Tn&&!(bn(i,s,d)&&new Date().getTime()-m>i.cooldownTime*.15);++_)s.tick(),_%5===0&&(A=Er(T+_,new Date().getTime()-m,i),postMessage({type:"tick",progress:A,elapsedTime:new Date().getTime()-m}));postMessage({type:"tick",progress:1,elapsedTime:new Date().getTime()-m}),((w=i.layout)==null?void 0:w.type)==="tree"&&$.simulationDone(o,l,s,i.layout),postMessage({type:"done",nodes:o.map(_=>_.toDict()),edges:l.map(_=>_.toDict())})};function dc(e,t,n,i){var m,A,b;const r=e.map(g=>{const w=new sr(g.id,g.getData(),g.getStyle());return w.weight=g.weight||1,w.setCircleRadius(g.getCircleRadius()),typeof g.x=="number"&&(w.x=g.x),typeof g.y=="number"&&(w.y=g.y),typeof g.fx=="number"&&(w.fx=g.fx),typeof g.fy=="number"&&(w.fy=g.fy),w}),o=new Map(r.map(g=>[g.id,g])),{simulation:a,simulationForces:s}=yn.initSimulationForces(n,i),c=[];for(const g of t){const w=o.get(g.from.id),_=o.get(g.to.id);if(w&&_){const R=g.getStyle()??{};c.push(new cn(g.id,w,_,g.getData(),R,g.directed))}}a.nodes(r);const l=a.force("link");l&&l.id(g=>g.id).links(c),(((m=n.layout)==null?void 0:m.type)==="tree"||((A=n.layout)==null?void 0:A.type)==="egoTree")&&$.registerForcesOnSimulation(r,c,a,s,n.layout,i,$);let f;n.warmupTicks==="auto"||n.warmupTicks==null?f=_n:f=n.warmupTicks,f=f-Tn;let T=.3;a.alphaTarget(T);const d=new Date().getTime();for(let g=0;g<f&&!(new Date().getTime()-d>vr||new Date().getTime()-d>n.cooldownTime||bn(n,a,T)&&new Date().getTime()-d>n.cooldownTime*.15);++g)a.tick();T=0,a.alphaTarget(T),a.alpha(1);for(let g=0;g<Tn&&!(bn(n,a,T)&&new Date().getTime()-d>n.cooldownTime*.15);++g)a.tick();return((b=n.layout)==null?void 0:b.type)==="tree"&&$.simulationDone(r,c,a,n.layout),{nodes:r,edges:c}}function Er(e,t,n){return t/n.cooldownTime}function bn(e,t,n){return e.d3AlphaMin>0&&t.alpha()-n<e.d3AlphaMin}var pc=Object.freeze({__proto__:null,runSimulation:dc})})();\n', Vi = typeof self < "u" && self.Blob && new Blob([Mr], { type: "text/javascript;charset=utf-8" });
function cl(e) {
  let t;
  try {
    if (t = Vi && (self.URL || self.webkitURL).createObjectURL(Vi), !t) throw "";
    const n = new Worker(t, {
      name: e == null ? void 0 : e.name
    });
    return n.addEventListener("error", () => {
      (self.URL || self.webkitURL).revokeObjectURL(t);
    }), n;
  } catch {
    return new Worker(
      "data:text/javascript;charset=utf-8," + encodeURIComponent(Mr),
      {
        name: e == null ? void 0 : e.name
      }
    );
  } finally {
    t && (self.URL || self.webkitURL).revokeObjectURL(t);
  }
}
function ul() {
  return new cl();
}
const hl = (e, t, n, i, r) => new Promise((o, a) => {
  const s = ul();
  s.postMessage({ source: "simulation-worker-wrapper", nodes: e, edges: t, options: n, canvasBCR: i }), s.onmessage = (c) => {
    const { type: l, progress: f, nodes: v, edges: d, elapsedTime: m } = c.data;
    if (l === "tick" && typeof f == "number") {
      r == null || r(f, m);
      return;
    }
    l === "done" && (o({ nodes: v, edges: d }), s.terminate());
  }, s.onerror = a;
});
var pn = typeof globalThis < "u" ? globalThis : typeof window < "u" ? window : typeof global < "u" ? global : typeof self < "u" ? self : {};
function fl(e) {
  return e && e.__esModule && Object.prototype.hasOwnProperty.call(e, "default") ? e.default : e;
}
var Ke = { exports: {} };
Ke.exports;
var $i;
function dl() {
  return $i || ($i = 1, (function(e, t) {
    var n = 200, i = "__lodash_hash_undefined__", r = 800, o = 16, a = 9007199254740991, s = "[object Arguments]", c = "[object Array]", l = "[object AsyncFunction]", f = "[object Boolean]", v = "[object Date]", d = "[object Error]", m = "[object Function]", S = "[object GeneratorFunction]", b = "[object Map]", g = "[object Number]", w = "[object Null]", _ = "[object Object]", R = "[object Proxy]", y = "[object RegExp]", C = "[object Set]", O = "[object String]", B = "[object Undefined]", P = "[object WeakMap]", L = "[object ArrayBuffer]", z = "[object DataView]", Y = "[object Float32Array]", nt = "[object Float64Array]", N = "[object Int8Array]", D = "[object Int16Array]", K = "[object Int32Array]", W = "[object Uint8Array]", M = "[object Uint8ClampedArray]", G = "[object Uint16Array]", Z = "[object Uint32Array]", ct = /[\\^$.*+?()[\]{}|]/g, gt = /^\[object .+?Constructor\]$/, it = /^(?:0|[1-9]\d*)$/, j = {};
    j[Y] = j[nt] = j[N] = j[D] = j[K] = j[W] = j[M] = j[G] = j[Z] = !0, j[s] = j[c] = j[L] = j[f] = j[z] = j[v] = j[d] = j[m] = j[b] = j[g] = j[_] = j[y] = j[C] = j[O] = j[P] = !1;
    var X = typeof pn == "object" && pn && pn.Object === Object && pn, Xt = typeof self == "object" && self && self.Object === Object && self, J = X || Xt || Function("return this")(), Zt = t && !t.nodeType && t, U = Zt && !0 && e && !e.nodeType && e, yt = U && U.exports === Zt, Qt = yt && X.process, Dt = (function() {
      try {
        var u = U && U.require && U.require("util").types;
        return u || Qt && Qt.binding && Qt.binding("util");
      } catch {
      }
    })(), Ae = Dt && Dt.isTypedArray;
    function Ee(u, p, T) {
      switch (T.length) {
        case 0:
          return u.call(p);
        case 1:
          return u.call(p, T[0]);
        case 2:
          return u.call(p, T[0], T[1]);
        case 3:
          return u.call(p, T[0], T[1], T[2]);
      }
      return u.apply(p, T);
    }
    function Xe(u, p) {
      for (var T = -1, F = Array(u); ++T < u; )
        F[T] = p(T);
      return F;
    }
    function Ze(u) {
      return function(p) {
        return u(p);
      };
    }
    function Ft(u, p) {
      return u == null ? void 0 : u[p];
    }
    function oe(u, p) {
      return function(T) {
        return u(p(T));
      };
    }
    var jt = Array.prototype, xe = Function.prototype, Jt = Object.prototype, Pt = J["__core-js_shared__"], Ht = xe.toString, St = Jt.hasOwnProperty, Ne = (function() {
      var u = /[^.]+$/.exec(Pt && Pt.keys && Pt.keys.IE_PROTO || "");
      return u ? "Symbol(src)_1." + u : "";
    })(), Re = Jt.toString, Qe = Ht.call(Object), Me = RegExp(
      "^" + Ht.call(St).replace(ct, "\\$&").replace(/hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g, "$1.*?") + "$"
    ), zt = yt ? J.Buffer : void 0, Wt = J.Symbol, At = J.Uint8Array;
    zt && zt.allocUnsafe;
    var fe = oe(Object.getPrototypeOf, Object), Ie = Object.create, Je = Jt.propertyIsEnumerable, Ce = jt.splice, Bt = Wt ? Wt.toStringTag : void 0, Kt = (function() {
      try {
        var u = Bn(Object, "defineProperty");
        return u({}, "", {}), u;
      } catch {
      }
    })(), de = zt ? zt.isBuffer : void 0, Et = Math.max, te = Date.now, pe = Bn(J, "Map"), Vt = Bn(Object, "create"), On = /* @__PURE__ */ (function() {
      function u() {
      }
      return function(p) {
        if (!ce(p))
          return {};
        if (Ie)
          return Ie(p);
        u.prototype = p;
        var T = new u();
        return u.prototype = void 0, T;
      };
    })();
    function Ot(u) {
      var p = -1, T = u == null ? 0 : u.length;
      for (this.clear(); ++p < T; ) {
        var F = u[p];
        this.set(F[0], F[1]);
      }
    }
    function De() {
      this.__data__ = Vt ? Vt(null) : {}, this.size = 0;
    }
    function kn(u) {
      var p = this.has(u) && delete this.__data__[u];
      return this.size -= p ? 1 : 0, p;
    }
    function se(u) {
      var p = this.__data__;
      if (Vt) {
        var T = p[u];
        return T === i ? void 0 : T;
      }
      return St.call(p, u) ? p[u] : void 0;
    }
    function Ln(u) {
      var p = this.__data__;
      return Vt ? p[u] !== void 0 : St.call(p, u);
    }
    function Fn(u, p) {
      var T = this.__data__;
      return this.size += this.has(u) ? 0 : 1, T[u] = Vt && p === void 0 ? i : p, this;
    }
    Ot.prototype.clear = De, Ot.prototype.delete = kn, Ot.prototype.get = se, Ot.prototype.has = Ln, Ot.prototype.set = Fn;
    function tt(u) {
      var p = -1, T = u == null ? 0 : u.length;
      for (this.clear(); ++p < T; ) {
        var F = u[p];
        this.set(F[0], F[1]);
      }
    }
    function ee() {
      this.__data__ = [], this.size = 0;
    }
    function Pn(u) {
      var p = this.__data__, T = le(p, u);
      if (T < 0)
        return !1;
      var F = p.length - 1;
      return T == F ? p.pop() : Ce.call(p, T, 1), --this.size, !0;
    }
    function tn(u) {
      var p = this.__data__, T = le(p, u);
      return T < 0 ? void 0 : p[T][1];
    }
    function Oe(u) {
      return le(this.__data__, u) > -1;
    }
    function en(u, p) {
      var T = this.__data__, F = le(T, u);
      return F < 0 ? (++this.size, T.push([u, p])) : T[F][1] = p, this;
    }
    tt.prototype.clear = ee, tt.prototype.delete = Pn, tt.prototype.get = tn, tt.prototype.has = Oe, tt.prototype.set = en;
    function $t(u) {
      var p = -1, T = u == null ? 0 : u.length;
      for (this.clear(); ++p < T; ) {
        var F = u[p];
        this.set(F[0], F[1]);
      }
    }
    function zn() {
      this.size = 0, this.__data__ = {
        hash: new Ot(),
        map: new (pe || tt)(),
        string: new Ot()
      };
    }
    function xt(u) {
      var p = un(this, u).delete(u);
      return this.size -= p ? 1 : 0, p;
    }
    function Yt(u) {
      return un(this, u).get(u);
    }
    function nn(u) {
      return un(this, u).has(u);
    }
    function rn(u, p) {
      var T = un(this, u), F = T.size;
      return T.set(u, p), this.size += T.size == F ? 0 : 1, this;
    }
    $t.prototype.clear = zn, $t.prototype.delete = xt, $t.prototype.get = Yt, $t.prototype.has = nn, $t.prototype.set = rn;
    function qt(u) {
      var p = this.__data__ = new tt(u);
      this.size = p.size;
    }
    function ge() {
      this.__data__ = new tt(), this.size = 0;
    }
    function ae(u) {
      var p = this.__data__, T = p.delete(u);
      return this.size = p.size, T;
    }
    function me(u) {
      return this.__data__.get(u);
    }
    function kt(u) {
      return this.__data__.has(u);
    }
    function on(u, p) {
      var T = this.__data__;
      if (T instanceof tt) {
        var F = T.__data__;
        if (!pe || F.length < n - 1)
          return F.push([u, p]), this.size = ++T.size, this;
        T = this.__data__ = new $t(F);
      }
      return T.set(u, p), this.size = T.size, this;
    }
    qt.prototype.clear = ge, qt.prototype.delete = ae, qt.prototype.get = me, qt.prototype.has = kt, qt.prototype.set = on;
    function sn(u, p) {
      var T = jn(u), F = !T && Un(u), q = !T && !F && yi(u), ot = !T && !F && !q && vi(u), st = T || F || q || ot, et = st ? Xe(u.length, String) : [], at = et.length;
      for (var Lt in u)
        st && // Safari 9 has enumerable `arguments.length` in strict mode.
        (Lt == "length" || // Node.js 0.10 has enumerable non-index properties on buffers.
        q && (Lt == "offset" || Lt == "parent") || // PhantomJS 2 has enumerable non-index properties on typed arrays.
        ot && (Lt == "buffer" || Lt == "byteLength" || Lt == "byteOffset") || // Skip index properties.
        gi(Lt, at)) || et.push(Lt);
      return et;
    }
    function ke(u, p, T) {
      (T !== void 0 && !hn(u[p], T) || T === void 0 && !(p in u)) && ne(u, p, T);
    }
    function an(u, p, T) {
      var F = u[p];
      (!(St.call(u, p) && hn(F, T)) || T === void 0 && !(p in u)) && ne(u, p, T);
    }
    function le(u, p) {
      for (var T = u.length; T--; )
        if (hn(u[T][0], p))
          return T;
      return -1;
    }
    function ne(u, p, T) {
      p == "__proto__" && Kt ? Kt(u, p, {
        configurable: !0,
        enumerable: !0,
        value: T,
        writable: !0
      }) : u[p] = T;
    }
    var ie = Fr();
    function k(u) {
      return u == null ? u === void 0 ? B : w : Bt && Bt in Object(u) ? Pr(u) : Hr(u);
    }
    function h(u) {
      return Fe(u) && k(u) == s;
    }
    function A(u) {
      if (!ce(u) || Ur(u))
        return !1;
      var p = Wn(u) ? Me : gt;
      return p.test($r(u));
    }
    function I(u) {
      return Fe(u) && _i(u.length) && !!j[k(u)];
    }
    function Q(u) {
      if (!ce(u))
        return jr(u);
      var p = mi(u), T = [];
      for (var F in u)
        F == "constructor" && (p || !St.call(u, F)) || T.push(F);
      return T;
    }
    function V(u, p, T, F, q) {
      u !== p && ie(p, function(ot, st) {
        if (q || (q = new qt()), ce(ot))
          rt(u, p, st, T, V, F, q);
        else {
          var et = F ? F(Gn(u, st), ot, st + "", u, p, q) : void 0;
          et === void 0 && (et = ot), ke(u, st, et);
        }
      }, bi);
    }
    function rt(u, p, T, F, q, ot, st) {
      var et = Gn(u, T), at = Gn(p, T), Lt = st.get(at);
      if (Lt) {
        ke(u, T, Lt);
        return;
      }
      var Rt = ot ? ot(et, at, T + "", u, p, st) : void 0, Pe = Rt === void 0;
      if (Pe) {
        var Kn = jn(at), Vn = !Kn && yi(at), wi = !Kn && !Vn && vi(at);
        Rt = at, Kn || Vn || wi ? jn(et) ? Rt = et : Yr(et) ? Rt = ln(et) : Vn ? (Pe = !1, Rt = Nt(at)) : wi ? (Pe = !1, Rt = ht(at)) : Rt = [] : qr(at) || Un(at) ? (Rt = et, Un(et) ? Rt = Xr(et) : (!ce(et) || Wn(et)) && (Rt = zr(at))) : Pe = !1;
      }
      Pe && (st.set(at, Rt), q(Rt, at, F, ot, st), st.delete(at)), ke(u, T, Rt);
    }
    function wt(u, p) {
      return Kr(Wr(u, p, Ti), u + "");
    }
    var Mt = Kt ? function(u, p) {
      return Kt(u, "toString", {
        configurable: !0,
        enumerable: !1,
        value: Qr(p),
        writable: !0
      });
    } : Ti;
    function Nt(u, p) {
      return u.slice();
    }
    function Le(u) {
      var p = new u.constructor(u.byteLength);
      return new At(p).set(new At(u)), p;
    }
    function ht(u, p) {
      var T = Le(u.buffer);
      return new u.constructor(T, u.byteOffset, u.length);
    }
    function ln(u, p) {
      var T = -1, F = u.length;
      for (p || (p = Array(F)); ++T < F; )
        p[T] = u[T];
      return p;
    }
    function cn(u, p, T, F) {
      var q = !T;
      T || (T = {});
      for (var ot = -1, st = p.length; ++ot < st; ) {
        var et = p[ot], at = void 0;
        at === void 0 && (at = u[et]), q ? ne(T, et, at) : an(T, et, at);
      }
      return T;
    }
    function Lr(u) {
      return wt(function(p, T) {
        var F = -1, q = T.length, ot = q > 1 ? T[q - 1] : void 0, st = q > 2 ? T[2] : void 0;
        for (ot = u.length > 3 && typeof ot == "function" ? (q--, ot) : void 0, st && Br(T[0], T[1], st) && (ot = q < 3 ? void 0 : ot, q = 1), p = Object(p); ++F < q; ) {
          var et = T[F];
          et && u(p, et, F, ot);
        }
        return p;
      });
    }
    function Fr(u) {
      return function(p, T, F) {
        for (var q = -1, ot = Object(p), st = F(p), et = st.length; et--; ) {
          var at = st[++q];
          if (T(ot[at], at, ot) === !1)
            break;
        }
        return p;
      };
    }
    function un(u, p) {
      var T = u.__data__;
      return Gr(p) ? T[typeof p == "string" ? "string" : "hash"] : T.map;
    }
    function Bn(u, p) {
      var T = Ft(u, p);
      return A(T) ? T : void 0;
    }
    function Pr(u) {
      var p = St.call(u, Bt), T = u[Bt];
      try {
        u[Bt] = void 0;
        var F = !0;
      } catch {
      }
      var q = Re.call(u);
      return F && (p ? u[Bt] = T : delete u[Bt]), q;
    }
    function zr(u) {
      return typeof u.constructor == "function" && !mi(u) ? On(fe(u)) : {};
    }
    function gi(u, p) {
      var T = typeof u;
      return p = p ?? a, !!p && (T == "number" || T != "symbol" && it.test(u)) && u > -1 && u % 1 == 0 && u < p;
    }
    function Br(u, p, T) {
      if (!ce(T))
        return !1;
      var F = typeof p;
      return (F == "number" ? Hn(T) && gi(p, T.length) : F == "string" && p in T) ? hn(T[p], u) : !1;
    }
    function Gr(u) {
      var p = typeof u;
      return p == "string" || p == "number" || p == "symbol" || p == "boolean" ? u !== "__proto__" : u === null;
    }
    function Ur(u) {
      return !!Ne && Ne in u;
    }
    function mi(u) {
      var p = u && u.constructor, T = typeof p == "function" && p.prototype || Jt;
      return u === T;
    }
    function jr(u) {
      var p = [];
      if (u != null)
        for (var T in Object(u))
          p.push(T);
      return p;
    }
    function Hr(u) {
      return Re.call(u);
    }
    function Wr(u, p, T) {
      return p = Et(p === void 0 ? u.length - 1 : p, 0), function() {
        for (var F = arguments, q = -1, ot = Et(F.length - p, 0), st = Array(ot); ++q < ot; )
          st[q] = F[p + q];
        q = -1;
        for (var et = Array(p + 1); ++q < p; )
          et[q] = F[q];
        return et[p] = T(st), Ee(u, this, et);
      };
    }
    function Gn(u, p) {
      if (!(p === "constructor" && typeof u[p] == "function") && p != "__proto__")
        return u[p];
    }
    var Kr = Vr(Mt);
    function Vr(u) {
      var p = 0, T = 0;
      return function() {
        var F = te(), q = o - (F - T);
        if (T = F, q > 0) {
          if (++p >= r)
            return arguments[0];
        } else
          p = 0;
        return u.apply(void 0, arguments);
      };
    }
    function $r(u) {
      if (u != null) {
        try {
          return Ht.call(u);
        } catch {
        }
        try {
          return u + "";
        } catch {
        }
      }
      return "";
    }
    function hn(u, p) {
      return u === p || u !== u && p !== p;
    }
    var Un = h(/* @__PURE__ */ (function() {
      return arguments;
    })()) ? h : function(u) {
      return Fe(u) && St.call(u, "callee") && !Je.call(u, "callee");
    }, jn = Array.isArray;
    function Hn(u) {
      return u != null && _i(u.length) && !Wn(u);
    }
    function Yr(u) {
      return Fe(u) && Hn(u);
    }
    var yi = de || Jr;
    function Wn(u) {
      if (!ce(u))
        return !1;
      var p = k(u);
      return p == m || p == S || p == l || p == R;
    }
    function _i(u) {
      return typeof u == "number" && u > -1 && u % 1 == 0 && u <= a;
    }
    function ce(u) {
      var p = typeof u;
      return u != null && (p == "object" || p == "function");
    }
    function Fe(u) {
      return u != null && typeof u == "object";
    }
    function qr(u) {
      if (!Fe(u) || k(u) != _)
        return !1;
      var p = fe(u);
      if (p === null)
        return !0;
      var T = St.call(p, "constructor") && p.constructor;
      return typeof T == "function" && T instanceof T && Ht.call(T) == Qe;
    }
    var vi = Ae ? Ze(Ae) : I;
    function Xr(u) {
      return cn(u, bi(u));
    }
    function bi(u) {
      return Hn(u) ? sn(u) : Q(u);
    }
    var Zr = Lr(function(u, p, T) {
      V(u, p, T);
    });
    function Qr(u) {
      return function() {
        return u;
      };
    }
    function Ti(u) {
      return u;
    }
    function Jr() {
      return !1;
    }
    e.exports = Zr;
  })(Ke, Ke.exports)), Ke.exports;
}
var pl = dl();
const yn = /* @__PURE__ */ fl(pl);
function gl(e) {
  var t = 0, n = e.children, i = n && n.length;
  if (!i) t = 1;
  else for (; --i >= 0; ) t += n[i].value;
  e.value = t;
}
function ml() {
  return this.eachAfter(gl);
}
function yl(e, t) {
  let n = -1;
  for (const i of this)
    e.call(t, i, ++n, this);
  return this;
}
function _l(e, t) {
  for (var n = this, i = [n], r, o, a = -1; n = i.pop(); )
    if (e.call(t, n, ++a, this), r = n.children)
      for (o = r.length - 1; o >= 0; --o)
        i.push(r[o]);
  return this;
}
function vl(e, t) {
  for (var n = this, i = [n], r = [], o, a, s, c = -1; n = i.pop(); )
    if (r.push(n), o = n.children)
      for (a = 0, s = o.length; a < s; ++a)
        i.push(o[a]);
  for (; n = r.pop(); )
    e.call(t, n, ++c, this);
  return this;
}
function bl(e, t) {
  let n = -1;
  for (const i of this)
    if (e.call(t, i, ++n, this))
      return i;
}
function Tl(e) {
  return this.eachAfter(function(t) {
    for (var n = +e(t.data) || 0, i = t.children, r = i && i.length; --r >= 0; ) n += i[r].value;
    t.value = n;
  });
}
function wl(e) {
  return this.eachBefore(function(t) {
    t.children && t.children.sort(e);
  });
}
function Sl(e) {
  for (var t = this, n = Al(t, e), i = [t]; t !== n; )
    t = t.parent, i.push(t);
  for (var r = i.length; e !== n; )
    i.splice(r, 0, e), e = e.parent;
  return i;
}
function Al(e, t) {
  if (e === t) return e;
  var n = e.ancestors(), i = t.ancestors(), r = null;
  for (e = n.pop(), t = i.pop(); e === t; )
    r = e, e = n.pop(), t = i.pop();
  return r;
}
function El() {
  for (var e = this, t = [e]; e = e.parent; )
    t.push(e);
  return t;
}
function xl() {
  return Array.from(this);
}
function Nl() {
  var e = [];
  return this.eachBefore(function(t) {
    t.children || e.push(t);
  }), e;
}
function Rl() {
  var e = this, t = [];
  return e.each(function(n) {
    n !== e && t.push({ source: n.parent, target: n });
  }), t;
}
function* Ml() {
  var e = this, t, n = [e], i, r, o;
  do
    for (t = n.reverse(), n = []; e = t.pop(); )
      if (yield e, i = e.children)
        for (r = 0, o = i.length; r < o; ++r)
          n.push(i[r]);
  while (n.length);
}
function Cn(e, t) {
  e instanceof Map ? (e = [void 0, e], t === void 0 && (t = Dl)) : t === void 0 && (t = Cl);
  for (var n = new Ye(e), i, r = [n], o, a, s, c; i = r.pop(); )
    if ((a = t(i.data)) && (c = (a = Array.from(a)).length))
      for (i.children = a, s = c - 1; s >= 0; --s)
        r.push(o = a[s] = new Ye(a[s])), o.parent = i, o.depth = i.depth + 1;
  return n.eachBefore(kl);
}
function Il() {
  return Cn(this).eachBefore(Ol);
}
function Cl(e) {
  return e.children;
}
function Dl(e) {
  return Array.isArray(e) ? e[1] : null;
}
function Ol(e) {
  e.data.value !== void 0 && (e.value = e.data.value), e.data = e.data.data;
}
function kl(e) {
  var t = 0;
  do
    e.height = t;
  while ((e = e.parent) && e.height < ++t);
}
function Ye(e) {
  this.data = e, this.depth = this.height = 0, this.parent = null;
}
Ye.prototype = Cn.prototype = {
  constructor: Ye,
  count: ml,
  each: yl,
  eachAfter: vl,
  eachBefore: _l,
  find: bl,
  sum: Tl,
  sort: wl,
  path: Sl,
  ancestors: El,
  descendants: xl,
  leaves: Nl,
  links: Rl,
  copy: Il,
  [Symbol.iterator]: Ml
};
function Ll(e, t) {
  return e.parent === t.parent ? 1 : 2;
}
function Jn(e) {
  var t = e.children;
  return t ? t[0] : e.t;
}
function ti(e) {
  var t = e.children;
  return t ? t[t.length - 1] : e.t;
}
function Fl(e, t, n) {
  var i = n / (t.i - e.i);
  t.c -= i, t.s += n, e.c += i, t.z += n, t.m += n;
}
function Pl(e) {
  for (var t = 0, n = 0, i = e.children, r = i.length, o; --r >= 0; )
    o = i[r], o.z += t, o.m += t, t += o.s + (n += o.c);
}
function zl(e, t, n) {
  return e.a.parent === t.parent ? e.a : n;
}
function _n(e, t) {
  this._ = e, this.parent = null, this.children = null, this.A = null, this.a = this, this.z = 0, this.m = 0, this.c = 0, this.s = 0, this.t = null, this.i = t;
}
_n.prototype = Object.create(Ye.prototype);
function Bl(e) {
  for (var t = new _n(e, 0), n, i = [t], r, o, a, s; n = i.pop(); )
    if (o = n._.children)
      for (n.children = new Array(s = o.length), a = s - 1; a >= 0; --a)
        i.push(r = n.children[a] = new _n(o[a], a)), r.parent = n;
  return (t.parent = new _n(null, 0)).children = [t], t;
}
function Gl() {
  var e = Ll, t = 1, n = 1, i = null;
  function r(l) {
    var f = Bl(l);
    if (f.eachAfter(o), f.parent.m = -f.z, f.eachBefore(a), i) l.eachBefore(c);
    else {
      var v = l, d = l, m = l;
      l.eachBefore(function(_) {
        _.x < v.x && (v = _), _.x > d.x && (d = _), _.depth > m.depth && (m = _);
      });
      var S = v === d ? 1 : e(v, d) / 2, b = S - v.x, g = t / (d.x + S + b), w = n / (m.depth || 1);
      l.eachBefore(function(_) {
        _.x = (_.x + b) * g, _.y = _.depth * w;
      });
    }
    return l;
  }
  function o(l) {
    var f = l.children, v = l.parent.children, d = l.i ? v[l.i - 1] : null;
    if (f) {
      Pl(l);
      var m = (f[0].z + f[f.length - 1].z) / 2;
      d ? (l.z = d.z + e(l._, d._), l.m = l.z - m) : l.z = m;
    } else d && (l.z = d.z + e(l._, d._));
    l.parent.A = s(l, d, l.parent.A || v[0]);
  }
  function a(l) {
    l._.x = l.z + l.parent.m, l.m += l.parent.m;
  }
  function s(l, f, v) {
    if (f) {
      for (var d = l, m = l, S = f, b = d.parent.children[0], g = d.m, w = m.m, _ = S.m, R = b.m, y; S = ti(S), d = Jn(d), S && d; )
        b = Jn(b), m = ti(m), m.a = l, y = S.z + _ - d.z - g + e(S._, d._), y > 0 && (Fl(zl(S, l, v), l, y), g += y, w += y), _ += S.m, g += d.m, R += b.m, w += m.m;
      S && !ti(m) && (m.t = S, m.m += _ - w), d && !Jn(b) && (b.t = d, b.m += g - R, v = l);
    }
    return v;
  }
  function c(l) {
    l.x *= t, l.y = l.depth * n;
  }
  return r.separation = function(l) {
    return arguments.length ? (e = l, r) : e;
  }, r.size = function(l) {
    return arguments.length ? (i = !1, t = +l[0], n = +l[1], r) : i ? null : [t, n];
  }, r.nodeSize = function(l) {
    return arguments.length ? (i = !0, t = +l[0], n = +l[1], r) : i ? [t, n] : null;
  }, r;
}
function Yi(e, t) {
  const n = new Set(t.map((i) => i.target.id));
  for (const i of e)
    if (!n.has(i.id)) return i;
  return e[0];
}
const Ul = 1e6;
function qi(e, t) {
  var s;
  const n = /* @__PURE__ */ new Map();
  for (const c of e)
    n.set(c.id, []);
  for (const c of t)
    (s = n.get(c.from.id)) == null || s.push(c.to);
  let i = 0, r = !1, o = null, a = -1;
  for (const c of e) {
    const l = /* @__PURE__ */ new Set([c.id]), f = [c];
    for (; f.length > 0 && !r; ) {
      const d = f.pop();
      for (const m of n.get(d.id) ?? []) {
        if (++i > Ul) {
          r = !0;
          break;
        }
        l.has(m.id) || (l.add(m.id), f.push(m));
      }
    }
    const v = l.size - 1;
    if (v > a && (a = v, o = c), r) break;
  }
  return r && console.warn("Pivotick: reachability search hit its traversal cap, using the best root found so far."), o ?? e[0];
}
function jl(e, t) {
  return Ir(e, t);
}
function Ir(e, t) {
  const n = /* @__PURE__ */ new Map(), i = /* @__PURE__ */ new Map();
  for (const l of e)
    n.set(l.id, []), i.set(l.id, 0);
  for (const l of t)
    l.directed !== !1 && (n.get(l.from.id).push(l.to), i.set(l.to.id, (i.get(l.to.id) || 0) + 1));
  const r = [], o = e.filter((l) => i.get(l.id) === 0);
  for (; o.length; ) {
    const l = o.shift();
    r.push(l);
    for (const f of n.get(l.id))
      i.set(f.id, i.get(f.id) - 1), i.get(f.id) === 0 && o.push(f);
  }
  if (r.length !== e.length)
    return console.warn("Pivotick: the graph has a cycle, so no shallowest root is defined — using the first node."), e[0];
  const a = /* @__PURE__ */ new Map();
  for (let l = r.length - 1; l >= 0; l--) {
    const f = r[l];
    let v = 0;
    for (const d of n.get(f.id))
      v = Math.max(v, 1 + (a.get(d.id) ?? 0));
    a.set(f.id, v);
  }
  let s = null, c = 1 / 0;
  for (const l of e) {
    const f = a.get(l.id);
    f < c && (c = f, s = l);
  }
  return s ?? e[0];
}
function Hl(e, t, n) {
  var v;
  const i = new Map(e.map((d) => [d.id, d])), r = new Map(e.map((d) => [d.id, []]));
  for (const d of t)
    !r.has(d.from.id) || !r.has(d.to.id) || (r.get(d.from.id).push(d.to.id), r.get(d.to.id).push(d.from.id));
  const o = (d) => {
    const m = /* @__PURE__ */ new Map([[d, 0]]), S = /* @__PURE__ */ new Map(), b = [d];
    for (let g = 0; g < b.length; g++) {
      const w = b[g];
      for (const _ of r.get(w) ?? [])
        m.has(_) || (m.set(_, m.get(w) + 1), S.set(_, w), b.push(_));
    }
    return { levels: m, parentOf: S, farthest: b[b.length - 1] };
  }, a = n !== void 0 && r.has(n) ? n : (v = e[0]) == null ? void 0 : v.id;
  if (a === void 0) return e[0];
  const s = o(a).farthest, { parentOf: c, farthest: l } = o(s), f = [];
  for (let d = l; d !== void 0; d = c.get(d))
    f.push(d);
  return i.get(f[Math.floor(f.length / 2)]) ?? e[0];
}
const Wl = 24, Kl = 16, ui = 1, Xi = 0.1;
function ei(e, t) {
  return e ? e.measured <= 0 ? An[1] : e.needed / e.measured * t : ui;
}
function ni(e) {
  if (!Number.isFinite(e)) return ui;
  const t = Math.ceil(e / Xi) * Xi;
  return Math.min(An[1], Math.max(ui, Math.round(t * 10) / 10));
}
function Vl(e) {
  const t = ei(e.level, e.current.levelSpacing);
  if (e.radial) {
    const n = ei(e.sibling, e.current.levelSpacing);
    return { levelSpacing: ni(Math.max(t, n)), siblingSpacing: e.current.siblingSpacing };
  }
  return {
    levelSpacing: ni(t),
    siblingSpacing: ni(ei(e.sibling, e.current.siblingSpacing))
  };
}
function $l(e, t) {
  return e + t + Wl;
}
function Yl(e, t) {
  return e + t + Kl;
}
const ql = 20, Zi = "__pivotick_forest_root__", Qi = "__pivotick_tree_spacer__", Ji = 4096, Xl = 5e4, tr = 0.5;
let er = "";
const ii = {
  type: "tree",
  rootId: void 0,
  parentKey: void 0,
  depthKey: void 0,
  rootIdAlgorithmFinder: "MaxReachability",
  strength: 0.25,
  radial: !1,
  radialGap: 750,
  spacing: "auto",
  levelSpacing: 1,
  siblingSpacing: 1,
  horizontal: !1
};
class $ {
  constructor(t, n, i, r = {}) {
    E(this, "graph");
    E(this, "simulation");
    E(this, "simulationForces");
    E(this, "options");
    E(this, "originalForceStrength");
    E(this, "canvasBCR");
    E(this, "levels");
    /** Deepest level in {@link levels}; the divisor turning `radialGap` into a ring gap. */
    E(this, "maxDepth", 0);
    /** Whether {@link update} re-derives the spacing multipliers; see {@link setSpacing}. */
    E(this, "autoSpacing");
    /** Nodes no edge touches, placed by {@link packParked} rather than by the hierarchy. */
    E(this, "parkedIds", /* @__PURE__ */ new Set());
    E(this, "positionedNodesByID");
    this.graph = t, this.simulation = n, this.simulationForces = i, this.options = yn({}, ii, r), this.originalForceStrength = {
      link: this.simulationForces.link.strength(),
      charge: this.simulationForces.charge.strength(),
      gravity: this.simulationForces.gravity.strength()
    }, this.autoSpacing = r.spacing === "auto" || r.spacing !== "manual" && r.levelSpacing === void 0 && r.siblingSpacing === void 0, this.positionedNodesByID = /* @__PURE__ */ new Map(), this.levels = /* @__PURE__ */ new Map(), this.setSizes(), this.update(), this.registerForces();
  }
  /**
   * Lay the tree out — and, while `spacing: 'auto'`, re-derive the multipliers from what the
   * nodes need and lay it out once more. Two passes rather than a loop: a gap scales linearly
   * with its multiplier, so the correction is exact. The second pass is skipped when it would
   * change nothing.
   */
  update() {
    var n, i;
    if (this.layoutOnce(), !this.autoSpacing || this.positionedNodesByID.size === 0) return;
    const t = Vl(this.measureAutoContext());
    t.levelSpacing === this.options.levelSpacing && t.siblingSpacing === this.options.siblingSpacing || (this.options.levelSpacing = t.levelSpacing, this.options.siblingSpacing = t.siblingSpacing, this.layoutOnce(), (i = (n = this.graph.UIManager) == null ? void 0 : n.physicsFlyout) == null || i.syncAutoSpacing(t));
  }
  layoutOnce() {
    const t = this.graph.getNodes(), n = this.graph.getEdges(), i = this.buildLevels(t, n, this.options), { levels: r, maxDepth: o, parked: a } = i;
    this.parkedIds = new Set(a);
    const { nodes: s, nodeById: c } = this.buildTree(t, n, this.options, this.canvasBCR, i);
    this.positionedNodesByID = c, this.levels = r, this.maxDepth = o, s && this.setNodePositions(s, this.options);
  }
  /**
   * The tightest pair on each axis of the tree as currently laid out, for
   * {@link tuneTreeSpacing}. Measured in *hierarchy* space (`x` = breadth or angle,
   * `y` = depth or radius), which is the layout's own answer, unpolluted by whatever
   * the force relaxation has since done to the free axis.
   */
  measureAutoContext() {
    const t = /* @__PURE__ */ new Map();
    for (const [o, a] of this.positionedNodesByID) {
      const s = this.graph.getMutableNode(o);
      if (!s || this.parkedIds.has(o)) continue;
      const c = s.expanded ? s.getCircleRadiusCollapsed() : s.getCircleRadius(), l = Number.isFinite(c) ? c : 0, f = t.get(a.depth) ?? [];
      f.push({ node: a, radius: l }), t.set(a.depth, f);
    }
    const n = [...t.keys()].sort((o, a) => o - a);
    let i = null, r = null;
    for (let o = 0; o < n.length; o++) {
      const a = t.get(n[o]), s = o + 1 < n.length ? t.get(n[o + 1]) : void 0;
      if (s) {
        const l = Math.max(n[o + 1] - n[o], 1), f = Math.abs((s[0].node.y ?? 0) - (a[0].node.y ?? 0)) / l, v = $l($.widestOf(a), $.widestOf(s));
        i = $.tighter(i, { measured: f, needed: v });
      }
      const c = [...a].sort((l, f) => (l.node.x ?? 0) - (f.node.x ?? 0));
      for (let l = 1; l < c.length; l++) {
        const [f, v] = [c[l - 1], c[l]], d = this.options.radial ? 2 * (v.node.y ?? 0) * Math.sin(Math.abs((v.node.x ?? 0) - (f.node.x ?? 0)) / 2) : (v.node.x ?? 0) - (f.node.x ?? 0), m = Yl(f.radius, v.radius);
        r = $.tighter(r, { measured: d, needed: m });
      }
    }
    return { level: i, sibling: r, radial: this.options.radial, current: this.getSpacing() };
  }
  /** The pair in the worse shape — the biggest shortfall relative to what it needs. */
  static tighter(t, n) {
    if (!t) return n;
    const i = (r) => r.needed / Math.max(r.measured, 1e-6);
    return i(n) > i(t) ? n : t;
  }
  static widestOf(t) {
    return t.reduce((n, i) => Math.max(n, i.radius), 0);
  }
  setSizes() {
    const t = this.graph.renderer.getCanvas();
    if (!t)
      throw new Error("Canvas element is not defined in the graph renderer.");
    this.canvasBCR = t.getBoundingClientRect();
  }
  setNodePositions(t, n) {
    for (const i of t) {
      const r = this.graph.getMutableNode(i.data.id);
      if (r)
        if (n.radial) {
          const o = i.x ?? 0, a = i.y ?? 0;
          r.x = a * Math.cos(o - Math.PI / 2), r.y = a * Math.sin(o - Math.PI / 2), r.fx = r.x, r.fy = r.y;
        } else n.horizontal ? (r.x = i.y, r.fx = i.y, r.y = i.x, delete r.fy) : (r.x = i.x, r.y = i.y, r.fy = i.y, delete r.fx);
    }
  }
  unsetNodePositions() {
    this.graph.getMutableNodes().forEach((t) => {
      delete t.fy, delete t.fx;
    });
  }
  registerForces() {
    const t = this.options.strength ?? 0.1;
    if (this.options.radial) {
      const n = $.radialRingGap(this.options, this.maxDepth), i = Ri(
        (r) => (this.levels.get(r.id) ?? 1) * n,
        0,
        0
      ).strength(t);
      this.simulation.force("tree-radial", i);
    } else
      this.simulation.force("tree-y", Ii((n) => {
        var i, r;
        return this.options.horizontal ? ((i = this.positionedNodesByID.get(n.id)) == null ? void 0 : i.x) ?? 0 : ((r = this.positionedNodesByID.get(n.id)) == null ? void 0 : r.y) ?? 0;
      }).strength(t)), this.simulation.force("tree-x", Mi((n) => {
        var i, r;
        return this.options.horizontal ? ((i = this.positionedNodesByID.get(n.id)) == null ? void 0 : i.y) ?? 0 : ((r = this.positionedNodesByID.get(n.id)) == null ? void 0 : r.x) ?? 0;
      }).strength(t));
    $.adjustOtherSimulationForces(this.simulationForces, this.options);
  }
  unregisterLayout() {
    this.unregisterForces(), this.unsetNodePositions();
  }
  unregisterForces() {
    this.simulation.force("tree-radial", null), this.simulation.force("tree-y", null), this.simulation.force("tree-x", null), $.resetOtherSimulationForces(this.simulationForces, this.originalForceStrength);
  }
  static registerForcesOnSimulation(t, n, i, r, o, a, s = this) {
    const c = yn({}, ii, o), l = c.strength ?? 0.1, f = a.width, v = a.height, d = [f / 2, v / 2], m = s.buildLevelsStatic(t, n, c), { levels: S, maxDepth: b } = m, { nodeById: g } = s.buildTreeStatic(t, n, c, a, m);
    if (c.radial) {
      const w = s.radialRingGap(c, b), _ = Ri(
        (R) => (S.get(R.id) ?? 1) * w,
        d[0],
        d[1]
      ).strength(l);
      i.force("tree-radial", _);
    } else
      i.force("tree-y", Ii((w) => {
        var _, R;
        return c.horizontal ? ((_ = g.get(w.id)) == null ? void 0 : _.x) ?? 0 : ((R = g.get(w.id)) == null ? void 0 : R.y) ?? 0;
      }).strength(l)), i.force("tree-x", Mi((w) => {
        var _, R;
        return c.horizontal ? ((_ = g.get(w.id)) == null ? void 0 : _.y) ?? 0 : ((R = g.get(w.id)) == null ? void 0 : R.x) ?? 0;
      }).strength(l));
    s.adjustOtherSimulationForces(r, c);
  }
  static adjustOtherSimulationForces(t, n) {
    n != null && n.radial ? (t.link.strength(0), t.charge.strength(0), t.gravity.strength(0)) : (t.link.strength(0), t.charge.strength(0), t.gravity.strength(1e-5));
  }
  static resetOtherSimulationForces(t, n) {
    t.link.strength(n.link), t.charge.strength(n.charge), t.gravity.strength(n.gravity);
  }
  static simulationDone(t, n, i, r) {
    const o = yn({}, ii, r);
    for (const a of t)
      o.radial ? (a.fx = a.x, a.fy = a.y) : o.horizontal ? (a.fx = a.x, delete a.fy) : (a.fy = a.y, delete a.fx);
  }
  /**
   * Distance between two consecutive rings in the radial layout. The layout sizes the tree to
   * `radialGap` and lets d3 spread `maxDepth` levels across it, so this must be the same
   * division or the radial *force* and the radial *positions* describe two different pictures
   * — invisible on the main thread, where pinned `fx`/`fy` outrank the force, but the worker
   * path is driven by the force alone.
   */
  static radialRingGap(t, n) {
    const i = t.radialGap * $.spacingOf(t).level;
    return n > 0 ? i / n : i;
  }
  /** The spacing multipliers in force, defaulted for a partially-specified options object. */
  static spacingOf(t) {
    const n = (i) => Number.isFinite(i) ? i : 1;
    return { level: n(t.levelSpacing), sibling: n(t.siblingSpacing) };
  }
  /** The spacing multipliers currently laid out. */
  getSpacing() {
    const { level: t, sibling: n } = $.spacingOf(this.options);
    return { levelSpacing: t, siblingSpacing: n };
  }
  /**
   * Re-lay-out at new spacing multipliers, keeping the root and orientation. The canvas is
   * re-measured first, being the length scale both multipliers work against.
   *
   * Re-registering the forces is not optional: `forceX`/`forceY`/`forceRadial` read their
   * per-node target once at initialize time, so recomputed positions alone leave every force
   * still pulling nodes to their old slots — levels spread, siblings snap back.
   */
  setSpacing(t) {
    this.autoSpacing = !1, this.options.spacing = "manual", t.levelSpacing !== void 0 && (this.options.levelSpacing = t.levelSpacing), t.siblingSpacing !== void 0 && (this.options.siblingSpacing = t.siblingSpacing), this.relayout();
  }
  /** Is the spacing tuning itself? */
  isAutoSpacing() {
    return this.autoSpacing;
  }
  /** The root the tree hangs from: a pinned node id, or the finder that picks one. */
  getRoot() {
    return { rootId: this.options.rootId, algorithm: this.options.rootIdAlgorithmFinder };
  }
  /**
   * Re-hang the tree from another root, keeping the orientation and the spacing. A `rootId`
   * pins the tree to that node and is walked ignoring edge direction, so any node gives a
   * whole tree; an `algorithm` drops the pin and lets the finder choose again. Goes through
   * {@link relayout} for the force-caching reason {@link setSpacing} gives.
   */
  setRoot(t) {
    "rootId" in t ? this.options.rootId = t.rootId : (this.options.rootId = void 0, this.options.rootIdAlgorithmFinder = t.algorithm), this.relayout();
  }
  /** Hand the multipliers back to the tuner and re-lay-out at what it picks. */
  enableAutoSpacing() {
    this.autoSpacing = !0, this.options.spacing = "auto", this.relayout();
  }
  relayout() {
    this.setSizes(), this.update(), this.positionedNodesByID.size !== 0 && this.registerForces();
  }
  /**
   * The d3 tree generator, sized for the canvas and the spacing multipliers, plus the offset
   * that re-centres the result on the box it would have filled at `1×`. A `size`d d3 tree is
   * normalised onto the whole box and grows from the top-left, so without the offset raising
   * a multiplier would push the tree off the bottom-right instead of expanding it in place.
   */
  static sizedTreeLayout(t, n) {
    const i = $.spacingOf(t), r = Gl();
    if (t.radial)
      return r.size([2 * Math.PI, t.radialGap * i.level]), { treeLayout: r, offset: { x: 0, y: 0 } };
    const o = t.horizontal ? n.width : n.height, a = t.horizontal ? n.height : n.width, s = a * i.sibling, c = o * i.level;
    return r.size([s, c]).separation((l, f) => {
      var d, m;
      const v = ((m = (d = l.parent) == null ? void 0 : d.children) == null ? void 0 : m.length) ?? 1;
      return l.parent === f.parent ? 1.5 / v : 1.5;
    }), {
      treeLayout: r,
      offset: {
        x: -(s - a) / 2,
        y: -(c - o) / 2
      }
    };
  }
  /**
   * Where to put the nodes with no relations at all. They have no place in a hierarchy, so
   * they go in the dead space a tree always leaves beside its shallow levels, one cell clear
   * of its silhouette.
   *
   * The *trailing* end, because the mode rail and its flyouts live down the left of the
   * canvas and nodes parked there would sit behind a panel. Inside the bounding box, because
   * the view is fitted and a stray dot outside it would zoom the whole tree out.
   *
   * Positions are in hierarchy space (`x` breadth, `y` depth — angle and radius when radial),
   * as everything `buildTreeStatic` returns.
   */
  static packParked(t, n, i, r, o = /* @__PURE__ */ new Map()) {
    if (!t.length) return [];
    const a = (M) => {
      const G = M.getCircleRadius();
      return Number.isFinite(G) ? G : 0;
    }, s = 2 * t.reduce((M, G) => Math.max(M, a(G)), 0) + ql, c = (M, G, Z, ct) => ({ data: M, depth: ct, x: G, y: Z, height: 0 }), l = n.map((M) => M.x ?? 0), f = n.map((M) => M.y ?? 0), v = t.filter((M) => o.has(M.id)), d = t.filter((M) => !o.has(M.id)), m = (M) => o.get(M.id) ?? 0, S = [...new Set(n.map((M) => M.depth))].sort((M, G) => M - G), b = /* @__PURE__ */ new Map();
    for (const M of n) b.set(M.depth, M.y ?? 0);
    const g = S[0] ?? 0, w = S[S.length - 1] ?? 0, _ = w > g ? ((b.get(w) ?? 0) - (b.get(g) ?? 0)) / (w - g) : s, R = (M) => b.get(M) ?? (S.length ? (b.get(g) ?? 0) + (M - g) * _ : M * s);
    if (i.radial) {
      const M = [], G = /* @__PURE__ */ new Map();
      for (const it of v) {
        const j = m(it);
        G.set(j, [...G.get(j) ?? [], it]);
      }
      for (const [it, j] of G) {
        const X = n.filter((U) => U.depth === it).map((U) => U.x ?? 0).sort((U, yt) => U - yt);
        let Xt = 0, J = 2 * Math.PI;
        if (X.length) {
          J = 0;
          for (let U = 0; U < X.length; U++) {
            const yt = U + 1 < X.length ? X[U + 1] : X[0] + 2 * Math.PI;
            yt - X[U] > J && (J = yt - X[U], Xt = X[U]);
          }
        }
        const Zt = J / (j.length + 1);
        j.forEach((U, yt) => M.push(
          c(U, Xt + Zt * (yt + 1), R(it), it)
        ));
      }
      const Z = new Set(f).size, ct = f.length ? Math.max(...f) : i.radialGap, gt = Z > 0 ? ct / Z : ct;
      return d.forEach((it, j) => M.push(c(
        it,
        j * 2 * Math.PI / d.length,
        ct + gt,
        Z + 1
      ))), M;
    }
    if (!n.length) {
      const M = Math.max(1, Math.floor(r.width / s)), G = [], Z = /* @__PURE__ */ new Map();
      for (const gt of v) {
        const it = m(gt), j = Z.get(it) ?? 0;
        Z.set(it, j + 1), G.push(c(gt, j * s, it * s, it));
      }
      const ct = Z.size ? Math.max(...Z.keys()) + 1 : 0;
      return d.forEach((gt, it) => {
        const j = ct + Math.floor(it / M);
        G.push(c(gt, it % M * s, j * s, j));
      }), G;
    }
    const y = /* @__PURE__ */ new Map(), C = /* @__PURE__ */ new Map();
    for (const M of n) {
      const G = M.y ?? 0, Z = M.x ?? 0;
      y.set(G, Math.max(y.get(G) ?? Z, Z)), C.set(M.depth, Math.max(C.get(M.depth) ?? Z, Z));
    }
    const O = [...y.keys()].sort((M, G) => M - G), B = Math.min(...l), P = Math.max(...l), L = O.length > 1 ? O[1] - O[0] : s, z = [], Y = [], nt = /* @__PURE__ */ new Map();
    for (const M of v) {
      const G = m(M), Z = R(G), ct = nt.get(Z) ?? 0, gt = P - ct * s, it = C.get(G);
      if (it !== void 0 && gt - s <= it) {
        Y.push(M);
        continue;
      }
      nt.set(Z, ct + 1), z.push(c(M, gt, Z, G));
    }
    const N = [...d, ...Y];
    let D = 0;
    for (const [M, G] of O.entries()) {
      if (D >= N.length) break;
      const Z = nt.get(G) ?? 0, ct = P - Z * s - ((y.get(G) ?? P) + s), gt = Math.floor(ct / s);
      for (let it = 0; it < gt && D < N.length; it++)
        z.push(c(N[D++], P - (Z + it) * s, G, M));
    }
    const K = Math.max(1, Math.floor((P - B) / s)), W = O[O.length - 1];
    for (let M = 0; D < N.length; M++)
      z.push(c(
        N[D++],
        P - M % K * s,
        W + L * (1 + Math.floor(M / K)),
        O.length
      ));
    return z;
  }
  /** Shift a laid-out tree in hierarchy space; see {@link sizedTreeLayout}. */
  static offsetTree(t, n) {
    if (!(!n.x && !n.y))
      for (const i of t)
        i.x = (i.x ?? 0) + n.x, i.y = (i.y ?? 0) + n.y;
  }
  buildTree(t, n, i, r, o) {
    return $.buildTreeStatic(t, n, i, r, o);
  }
  /** Is this one of the layout's own scaffolding nodes rather than a node of the graph? */
  static isScaffolding(t) {
    return t === Zi || t.startsWith(Qi);
  }
  static buildTreeStatic(t, n, i, r, o) {
    if (!t.length)
      return {
        root: null,
        nodes: [],
        nodeById: /* @__PURE__ */ new Map()
      };
    const a = /* @__PURE__ */ new Map();
    for (const L of t) {
      const z = L;
      z.children = [], a.set(L.id, z);
    }
    const { parentOf: s, roots: c, parked: l, levels: f, declaredRows: v } = o ?? $.buildLevelsStatic(t, n, i);
    let d = 0;
    const m = () => ({ id: `${Qi}${d++}`, children: [] }), S = (L) => f.get(L) ?? 0, b = (L, z, Y, nt) => {
      let N = L;
      for (let D = z + 1; D < nt && d < Xl; D++) {
        const K = m();
        N.children.push(K), N = K;
      }
      N.children.push(Y);
    };
    for (const [L, z] of s) {
      const Y = a.get(L), nt = a.get(z);
      !Y || !nt || (b(nt, S(z), Y, S(L)), Y.parent = nt);
    }
    const g = l.map((L) => a.get(L)).filter((L) => !!L), w = $.hierarchyRootFor(c, a, S, b, m);
    if (!w) {
      if (!c.length && g.length) {
        const L = $.packParked(g, [], i, r, v);
        return {
          root: null,
          nodes: L,
          nodeById: new Map(L.map((z) => [z.data.id, z]))
        };
      }
      throw new Error(`Root node with id "${c[0]}" not found.`);
    }
    const { treeLayout: _, offset: R } = $.sizedTreeLayout(i, r), y = Cn(w), C = _(y);
    $.offsetTree(C.descendants(), R);
    const O = C.descendants().filter((L) => !$.isScaffolding(L.data.id)), B = $.packParked(g, O, i, r, v), P = /* @__PURE__ */ new Map();
    for (const L of B) P.set(L.data.id, L);
    return C.descendants().forEach((L) => {
      $.isScaffolding(L.data.id) || P.set(L.data.id, L);
    }), {
      root: C,
      nodes: [...O, ...B],
      nodeById: P
    };
  }
  /**
   * The node to hang the hierarchy off. One component roots the tree at its own root; several
   * make a *forest*, which gets a synthetic root holding one component per child so they lay
   * out side by side. That root is dropped from everything returned.
   *
   * A root starting further down needs the rows above it, so it gets a spacer chain too — in
   * a forest hanging off the synthetic root (the `forestShift` in `buildLevelsStatic`), and
   * otherwise with the top of its own chain standing in as the hierarchy root.
   */
  static hierarchyRootFor(t, n, i, r, o) {
    if (t.length === 1) {
      const c = n.get(t[0]);
      if (!c || i(t[0]) <= 0) return c;
      const l = o();
      return r(l, 0, c, i(t[0])), l;
    }
    const a = t.map((c) => n.get(c)).filter((c) => !!c);
    if (!a.length) return;
    const s = { id: Zi, children: [] };
    for (const c of a) r(s, 0, c, i(c.id));
    return s;
  }
  buildLevels(t, n, i) {
    return $.buildLevelsStatic(t, n, i);
  }
  /**
   * What the caller stated about the hierarchy, read off `node.data` per `parentKey` and
   * `depthKey`, with everything unusable already dropped.
   */
  static readDeclaredHierarchy(t, n, i) {
    const r = /* @__PURE__ */ new Map(), o = /* @__PURE__ */ new Map(), a = /* @__PURE__ */ new Map(), s = (d) => a.set(d, (a.get(d) ?? 0) + 1), c = i ? n.parentKey : void 0, l = n.depthKey;
    if (!c && !l) return { parentOf: r, rowOf: o, complaints: a };
    const f = new Set(t.map((d) => d.id));
    for (const d of t) {
      const m = d.getData();
      if (l) {
        const g = m[l];
        if (g != null && g !== "") {
          const w = Math.floor(Number(g));
          Number.isFinite(w) && w >= 0 && w <= Ji ? o.set(d.id, w) : s(`declared depths that are not a row between 0 and ${Ji}`);
        }
      }
      if (!c) continue;
      const S = m[c];
      if (S == null || S === "") continue;
      const b = String(S);
      b === d.id ? s("declared parents pointing at their own node") : f.has(b) ? r.set(d.id, b) : s("declared parents not in the layout");
    }
    const v = /* @__PURE__ */ new Set();
    for (const d of [...r.keys()]) {
      if (v.has(d)) continue;
      const m = [], S = /* @__PURE__ */ new Set();
      let b = d;
      for (; b !== void 0 && !v.has(b); ) {
        if (S.has(b)) {
          r.delete(b), s("declared parent cycles broken");
          break;
        }
        S.add(b), m.push(b), b = r.get(b);
      }
      for (const g of m) v.add(g);
    }
    return { parentOf: r, rowOf: o, complaints: a };
  }
  /** One line per layout pass, however many things the declared hierarchy got wrong. */
  static warnAboutDeclared(t) {
    if (!t.size) return;
    const n = "[Pivotick] Tree layout ignored part of the declared hierarchy: " + [...t].map(([i, r]) => `${r} ${i}`).join(", ") + ".";
    n !== er && (er = n, console.warn(n));
  }
  /**
   * The hierarchy the layout will draw: one parent per node, the row each sits on, and the
   * roots it all hangs from.
   *
   * Parenthood comes from a BFS over the edges — the first edge to reach a node is its
   * parent — except where the caller stated it through `parentKey`, honoured whether or not
   * an edge joins the pair. Rows are one-below-the-parent unless `depthKey` asks for a lower
   * one; a row above the parent's is clamped, since detaching the node instead would let one
   * bad number shatter the tree into extra components.
   *
   * If the graph contains cycles, each node is assigned the shortest level found first.
   *
   * @param nodes - The list of graph nodes.
   * @param edges - The list of graph edges (assumed to be directed).
   * @param options - The layout options. `rootId` is ignored when no such node is in
   *   `nodes`; when it is, the walk follows edges in either direction — as it also does for
   *   a *found* root whose arrows cannot cover the graph, see {@link MIN_DIRECTED_COVERAGE}
   *   — and `parentKey` is dropped with it, because pinning a root is a request to re-hang
   *   the tree from there.
   * @returns A mapping of each node's ID to its depth level in the tree and the maximum depth
   */
  static buildLevelsStatic(t, n, i = {}) {
    var nt;
    if (!t.length)
      return {
        levels: /* @__PURE__ */ new Map(),
        maxDepth: 0,
        nodeCountPerLevel: {},
        parentOf: /* @__PURE__ */ new Map(),
        roots: [],
        parked: [],
        declaredRows: /* @__PURE__ */ new Map()
      };
    const r = i.rootId !== void 0 && t.some((N) => N.id === i.rootId) ? i.rootId : void 0, o = $.readDeclaredHierarchy(t, i, r === void 0);
    let a = r !== void 0;
    const s = /* @__PURE__ */ new Map(), c = new Map(o.parentOf), l = /* @__PURE__ */ new Map(), f = /* @__PURE__ */ new Set(), v = /* @__PURE__ */ new Set();
    for (const N of t)
      l.set(N.id, []);
    for (const { source: N, target: D } of n)
      (nt = l.get(N.id)) == null || nt.push(D.id), f.add(D.id), v.add(N.id), v.add(D.id);
    for (const [N, D] of o.parentOf)
      v.add(N), v.add(D), f.add(N);
    const d = () => {
      var N;
      for (const { source: D, target: K } of n) (N = l.get(K.id)) == null || N.push(D.id);
      a = !0;
    };
    a && d();
    const m = (N) => v.has(N) || N === r, S = t.filter((N) => m(N.id)), b = t.filter((N) => !m(N.id)).map((N) => N.id), g = o.parentOf.size > 0, w = (N, D) => {
      const K = /* @__PURE__ */ new Set();
      let W = D;
      for (; W !== void 0 && !K.has(W); ) {
        if (W === N) return !0;
        K.add(W), W = c.get(W);
      }
      return !1;
    }, _ = /* @__PURE__ */ new Set(), R = (N) => {
      if (_.has(N)) return;
      _.add(N);
      const D = [N];
      let K = 0;
      for (; K < D.length; ) {
        const W = D[K++];
        for (const M of l.get(W) ?? [])
          _.has(M) || (_.add(M), !c.has(M) && !(g && w(M, W)) && c.set(M, W), D.push(M));
      }
    }, y = [];
    if (S.length) {
      const N = g ? S.filter((W) => !c.has(W.id)) : S, D = N.length ? N : S;
      let K = r ?? $.findRootId(D, n, i.rootIdAlgorithmFinder);
      if (r === void 0 && $.directedCoverage(K, l, n) < tr) {
        const W = qi(D, n).id;
        $.directedCoverage(W, l, n) < tr && (d(), K = Hl(D, n, W).id);
      }
      R(K);
      for (const W of S) {
        if (_.has(W.id)) continue;
        const M = S.find((G) => !_.has(G.id) && !f.has(G.id)) ?? W;
        R(M.id);
      }
      c.has(K) || y.push(K);
      for (const W of S)
        W.id !== K && !c.has(W.id) && y.push(W.id);
    }
    const C = /* @__PURE__ */ new Map();
    for (const [N, D] of c) {
      const K = C.get(D) ?? [];
      K.push(N), C.set(D, K);
    }
    let O = 0;
    const B = (N, D) => {
      const K = [[N, D]];
      for (; K.length; ) {
        const [W, M] = K.pop();
        if (s.has(W)) continue;
        const G = o.rowOf.get(W);
        G !== void 0 && G < M && O++;
        const Z = G !== void 0 && G > M ? G : M;
        s.set(W, Z);
        for (const ct of C.get(W) ?? []) K.push([ct, Z + 1]);
      }
    };
    for (const N of y) B(N, 0);
    for (const N of S)
      s.has(N.id) || B(N.id, 0);
    O && o.complaints.set("declared depths clamped to just below their parent", O), $.warnAboutDeclared(o.complaints);
    const P = y.length > 1 ? 1 : 0;
    if (P)
      for (const [N, D] of s) s.set(N, D + P);
    let L = 0;
    for (const N of s.values())
      N > L && (L = N);
    if (b.length) {
      const N = L + 1;
      for (const D of b) {
        const K = o.rowOf.get(D);
        s.set(D, K === void 0 ? N : K + P);
      }
      for (const D of s.values())
        D > L && (L = D);
    }
    const z = /* @__PURE__ */ new Map();
    for (const [N, D] of o.rowOf) z.set(N, D + P);
    const Y = {};
    for (const N of s.values())
      Y[N] = (Y[N] || 0) + 1;
    return {
      levels: s,
      maxDepth: L,
      nodeCountPerLevel: Y,
      parentOf: c,
      roots: y,
      parked: b,
      declaredRows: z
    };
  }
  /**
   * The share of `root`'s own component that `root` reaches by following the arrows — the test
   * behind {@link MIN_DIRECTED_COVERAGE}. Scored against the component, not the whole graph:
   * several separate hierarchies are *supposed* to come out as a forest, and scoring against
   * every node would read that as a failure and throw away perfectly good arrows.
   *
   * @param adj - Adjacency in its **directed** reading, before any reverse links.
   */
  static directedCoverage(t, n, i) {
    var s, c;
    const r = (l) => {
      const f = /* @__PURE__ */ new Set([t]), v = [t];
      for (let d = 0; d < v.length; d++)
        for (const m of l.get(v[d]) ?? [])
          f.has(m) || (f.add(m), v.push(m));
      return f.size;
    }, o = /* @__PURE__ */ new Map();
    for (const l of n.keys()) o.set(l, []);
    for (const { source: l, target: f } of i)
      (s = o.get(l.id)) == null || s.push(f.id), (c = o.get(f.id)) == null || c.push(l.id);
    const a = r(o);
    return a === 0 ? 1 : r(n) / a;
  }
  /**
   * Attempts to infer the root node of a directed graph.
   *
   * This function looks for a node that is never a target in the list of links,
   * assuming such a node is a likely root (i.e., has no incoming edges).
   * If no such node is found, it falls back to the first node in the list.
   *
   * @param nodes - The list of graph nodes.
   * @param edges - The list of graph edges (assumed to be directed).
   * @returns The ID of the inferred root node.
   */
  static findRootId(t, n, i) {
    switch (i) {
      case "FirstZeroInDegree":
        return Yi(t, n).id;
      case "MaxReachability":
        return qi(t, n).id;
      case "MinMaxDistance":
        return jl(t, n).id;
      case "MinHeight":
        return Ir(t, n).id;
      default:
        return Yi(t, n).id;
    }
  }
}
class ue extends $ {
  constructor(t, n, i, r) {
    super(t, n, i, {
      ...r,
      type: "tree"
    });
  }
  static registerForcesOnSimulation(t, n, i, r, o, a) {
    $.registerForcesOnSimulation(
      t,
      n,
      i,
      r,
      o,
      a,
      ue
    );
  }
  buildTree(t, n, i, r) {
    return ue.buildTreeStatic(t, n, i, r);
  }
  static buildTreeStatic(t, n, i, r) {
    if (!t.length)
      return {
        root: null,
        nodes: [],
        nodeById: /* @__PURE__ */ new Map()
      };
    const o = /* @__PURE__ */ new Map();
    for (const S of t) {
      const b = S;
      b.children = [], o.set(S.id, b);
    }
    if (!i.rootId || !o.get(i.rootId))
      throw new Error("Ego Tree can only be created with a rootId");
    const a = i.rootId, s = o.get(a);
    if (s.children = [], !s)
      throw new Error(`Root node with id "${a}" not found.`);
    const c = /* @__PURE__ */ new Set([s.id]);
    for (const S of n) {
      const b = o.get(S.source.id), g = o.get(S.target.id);
      if (!b || !g) continue;
      const w = S.source.id === s.id ? g : S.target.id === s.id ? b : void 0;
      !w || c.has(w.id) || (c.add(w.id), s.children.push(w), w.parent = s);
    }
    const { treeLayout: l, offset: f } = ue.sizedTreeLayout(i, r), v = Cn(s), d = l(v);
    ue.offsetTree(d.descendants(), f);
    const m = /* @__PURE__ */ new Map();
    return d.descendants().forEach((S) => {
      m.set(S.data.id, S);
    }), {
      root: d,
      nodes: d.descendants(),
      nodeById: m
    };
  }
}
function Zl(e) {
  var n;
  const t = (n = e.getData()) == null ? void 0 : n.label;
  return typeof t == "string" ? t : "";
}
const nr = 0.3, Ql = 1, ir = 4, Jl = 400, tc = 24, ec = 6.5, nc = 10, ic = 140, rc = 1.15, oc = 0.35, sc = 0.35, ac = 58e-4, lc = 38, Cr = 95, cc = 300, uc = 0.35, hc = 8, fc = 10, dc = 0.54, rr = 24, pc = 62, gc = 0.2, mc = 240, yc = 0.9, gn = 1e-3, _c = 0.06, vc = 0.03, bc = 8, or = 2e-3;
function Se(e, t, n) {
  return Math.max(t, Math.min(n, e));
}
function qe(e) {
  return Se(e, 0, 1);
}
function Dn(e, t) {
  const [n, i] = lt[t];
  return Se(e, n, i);
}
function Tc(e) {
  return nr + (Ql - nr) * Dr(e);
}
function Dr(e) {
  return qe(
    Math.log10(Math.max(e, 1) / ir) / Math.log10(Jl / ir)
  );
}
function Or(e) {
  const t = 10 + Math.sqrt(Math.max(0, e - 10));
  return t * t / 100;
}
function wc(e, t) {
  const n = Math.pow(Math.max(1, t) / fc, dc), i = lc * Math.pow(cc / Math.max(1, e), uc);
  return Se(i * n, hc, Cr);
}
function Sc(e, t, n) {
  const i = e / Or(t);
  return Se(i / 400 * 100, wc(n, t), Cr);
}
function Ac(e, t) {
  return e / 100 * 400 * Or(t);
}
function Ec(e) {
  const t = (e - 0.6) / 1.7999999999999998;
  return Dn(4 + t * 56, "collisionRadius");
}
function xc(e, t, n, i) {
  const r = Math.max(1, yc * 0.5 * Math.min(t.width, t.height)), o = mc * n * e / (r * r * r), a = gn + (vc - gn) * Math.pow(1 - Dr(n), 3), s = gn + (_c - gn) * qe(i), c = Math.max(a, s), l = Se(o, or, Math.max(or, c));
  return Dn(100 * Math.sqrt(qe(l / gc)), "centering");
}
function Nc(e) {
  return Dn(1.2 + 0.8 * Math.log10(Math.max(e, 1)), "settleTime");
}
function Rc(e) {
  const t = qe(Math.log10(Math.max(e, 1) / 4) / Math.log10(125));
  return rr + (pc - rr) * t;
}
function Mc(e) {
  return {
    repulsion: Math.round(e.repulsion),
    linkDistance: Math.round(e.linkDistance),
    collisionRadius: Math.round(e.collisionRadius),
    friction: Math.round(e.friction),
    centering: Math.round(e.centering),
    settleTime: Math.round(e.settleTime * 10) / 10
  };
}
function Ic(e) {
  const t = Tc(e.nodeCount) * e.canvas.width * e.canvas.height;
  return { targetArea: t, spacing: Math.sqrt(t / Math.max(1, e.nodeCount)) };
}
function Cc(e) {
  const { spacing: t } = Ic(e), n = Math.max(1, e.radii.mean), i = Math.max(0.8 * t, ec * n), r = 2 * n + tc, o = Math.min(lt.linkDistance[1], nc * n + ic), a = Dn(Se(i, r, Math.max(r, o)), "linkDistance"), s = ac * t * t, c = e.radii.totalArea / Math.max(1, e.nodeCount * a * a), l = rc + oc * qe(c / sc), f = Sc(s, n, e.nodeCount);
  return Mc({
    repulsion: f,
    linkDistance: a,
    collisionRadius: Ec(l),
    friction: Rc(e.nodeCount),
    centering: xc(Ac(f, n), e.canvas, e.nodeCount, e.looseNodeFraction),
    settleTime: Nc(e.nodeCount)
  });
}
function Dc(e, t) {
  const n = /* @__PURE__ */ new Map();
  for (const a of e) n.set(a, a);
  const i = (a) => {
    let s = a;
    for (; n.get(s) !== s; ) s = n.get(s);
    let c = a;
    for (; n.get(c) !== s; ) {
      const l = n.get(c);
      n.set(c, s), c = l;
    }
    return s;
  };
  let r = e.length;
  for (const [a, s] of t) {
    if (!n.has(a) || !n.has(s)) continue;
    const c = i(a), l = i(s);
    c !== l && (n.set(c, l), r--);
  }
  let o = 0;
  if (r > 1) {
    const a = /* @__PURE__ */ new Map();
    for (const s of e) {
      const c = i(s);
      a.set(c, (a.get(c) ?? 0) + 1);
    }
    for (const s of a.values())
      s < bc && (o += s);
  }
  return { count: r, looseNodeFraction: e.length ? o / e.length : 0 };
}
const Ue = {
  d3Alpha: 1,
  d3AlphaMin: 1e-3,
  d3AlphaDecay: 0.05,
  d3AlphaTarget: 0,
  d3VelocityDecay: 0.45,
  d3LinkDistance: 40,
  d3LinkStrength: null,
  d3ManyBodyStrength: -150,
  d3ManyBodyTheta: 0.9,
  d3CollideRadius: 12,
  d3CollideRadiusMultiplier: 1.2,
  d3CollideStrength: 1,
  d3CollideIterations: 1,
  d3GravityStrength: 0.1,
  d3GravityStrengthConnected: 1e-3,
  enabled: !0,
  cooldownTime: 2e3,
  useWorker: !0,
  warmupTicks: "auto",
  freezeNodesOnDrag: !0,
  gridSnappingEnabled: !1,
  gridSize: 50,
  fitViewOnExpandCollapse: !1,
  layout: {
    type: "force"
  },
  callbacks: {
    onInit: () => {
    },
    onStart: () => {
    },
    onStop: () => {
    },
    onTick: () => {
    }
  }
}, lt = {
  repulsion: [0, 100],
  linkDistance: [40, 600],
  collisionRadius: [4, 60],
  friction: [0, 100],
  centering: [0, 100],
  settleTime: [0.5, 8]
}, An = [0.5, 10], Oc = "MaxReachability", kc = { levelSpacing: 1, siblingSpacing: 1 }, Lc = {
  tight: { repulsion: 32, linkDistance: 70, collisionRadius: 16, friction: 45, centering: 7, settleTime: 3 },
  loose: { repulsion: 70, linkDistance: 150, collisionRadius: 26, friction: 28, centering: 7, settleTime: 2.25 }
}, x = class x {
  constructor(t, n = {}) {
    E(this, "simulation");
    E(this, "graph");
    E(this, "container");
    E(this, "graphInteraction");
    E(this, "layout");
    /**
     * The area the physics tunes itself against: the **root container**, never the canvas.
     * Chrome opening or closing (a sidebar, the data dock) resizes the canvas, and a layout
     * has to come out the same either way. Every site reads this one snapshot, kept in step
     * with real container resizes by {@link observeContainer}.
     */
    E(this, "containerBCR");
    E(this, "containerObserver");
    E(this, "animationFrameId", null);
    E(this, "startSimulationTime", 0);
    E(this, "engineRunning", !1);
    E(this, "slowTickThresholdReached", !1);
    E(this, "avgTickDuration", 0);
    E(this, "SLOW_TICK_THRESHOLD", 33);
    // ms of tick compute+render (≈30fps budget)
    E(this, "dragInProgress", !1);
    E(this, "dragSelection", []);
    E(this, "totalTickCount", 0);
    /** Ticks since the current run started ({@link restart}); the cooldown budget. */
    E(this, "runTickCount", 0);
    E(this, "options");
    E(this, "callbacks");
    E(this, "simulationForces");
    E(this, "scaledForces", {
      d3ManyBodyStrength: Ue.d3ManyBodyStrength,
      d3CollideStrength: Ue.d3CollideStrength
    });
    /** Current abstract physics-knob values (what the View flyout renders). */
    E(this, "physicsKnobs");
    // ─── Auto tuner ─────────────────────────────────────────────────────────
    /** Whether the `Auto` preset is driving the knobs (see the constructor for how this is decided). */
    E(this, "autoEnabled");
    E(this, "autoTuneTimer", null);
    /** Set while auto writes knobs, so its own setter calls don't read as a manual edit. */
    E(this, "applyingAutoKnobs", !1);
    /** Set while auto writes knobs, so six setters produce one reheat rather than six. */
    E(this, "suppressReheat", !1);
    /** Last context + knobs auto computed. */
    E(this, "autoLastRun", null);
    if (this.graph = t, this.autoEnabled = x.shouldAutoTune(n), this.options = yn({}, Ue, n), this.callbacks = this.options.callbacks ?? {}, this.physicsKnobs = x.knobsFromOptions(this.options), this.container = this.graph.renderer.getRootContainer(), !this.container) throw new Error("Root container is not defined in the graph renderer.");
    if (this.containerBCR = x.measureContainer(this.container), this.graphInteraction = this.graph.renderer.getGraphInteraction(), !this.graphInteraction) throw new Error("Graph interaction is not available.");
    const i = x.initSimulationForces(this.options, this.containerBCR);
    this.simulation = i.simulation, this.simulationForces = i.simulationForces, this.scaledForces.d3ManyBodyStrength = this.options.d3ManyBodyStrength || Ue.d3ManyBodyStrength, this.scaledForces.d3CollideStrength = this.options.d3CollideStrength || Ue.d3CollideStrength, this.options.layout.type === "tree" ? this.layout = new $(
      this.graph,
      this.simulation,
      this.simulationForces,
      this.options.layout
    ) : this.options.layout.type === "egoTree" && (this.layout = new ue(
      this.graph,
      this.simulation,
      this.simulationForces,
      this.options.layout
    )), this.layout && Object.assign(this.options.layout, this.layout.getSpacing()), this.observeContainer(), this.callbacks.onInit && this.callbacks.onInit(this);
  }
  /** Measure a container, substituting {@link FALLBACK_CONTAINER_SIZE} for a zero area. */
  static measureContainer(t) {
    const n = t.getBoundingClientRect();
    if (n.width > 0 && n.height > 0) return n;
    const { width: i, height: r } = x.FALLBACK_CONTAINER_SIZE;
    return new DOMRect(n.x, n.y, i, r);
  }
  /**
   * Keep {@link containerBCR} in step with genuine container resizes — a window resize,
   * a responsive parent, or the container getting its first real size after being built
   * hidden — and re-aim the forces derived from it. Chrome resizes the *canvas*, not the
   * container, so opening the data dock or collapsing the sidebar never reaches here.
   */
  observeContainer() {
    !this.container || typeof ResizeObserver > "u" || (this.containerObserver = new ResizeObserver(() => {
      if (!this.container) return;
      const t = x.measureContainer(this.container);
      t.width === this.containerBCR.width && t.height === this.containerBCR.height || (this.containerBCR = t, x.initSimulationForceGravity(this.simulationForces.gravity, this.options, t), this.scheduleTune());
    }), this.containerObserver.observe(this.container));
  }
  /** Stop the engine and release the container observer. */
  destroy() {
    var t;
    this.stop(), (t = this.containerObserver) == null || t.disconnect(), this.containerObserver = void 0, this.container = void 0;
  }
  /** @private */
  static initSimulationForces(t, n) {
    const i = {
      link: So(),
      charge: Bo(),
      collide: To(),
      gravity: Go()
      // clusterRadialConstraint: ForceClusterRadial(),
    }, r = zo().force("link", i.link).force("charge", i.charge).force("collide", i.collide).force("gravity", i.gravity);
    return this.initSimulationForceGravity(i.gravity, t, n), this.initSimulationForceLink(i.link, t), this.initSimulationForceCharge(i.charge, t), this.initSimulationForceCollide(i.collide, t), r.alphaMin(t.d3AlphaMin), r.alphaDecay(t.d3AlphaDecay), r.alphaTarget(0), r.velocityDecay(t.d3VelocityDecay), {
      simulation: r,
      simulationForces: i
    };
  }
  static initSimulationForceGravity(t, n, i) {
    t.x(i.width / 2).y(i.height / 2).strength((r) => (r.degree() ?? 0) === 0 ? n.d3GravityStrength : n.d3GravityStrengthConnected);
  }
  static initSimulationForceLink(t, n) {
    t.distance((i) => {
      const r = i.__clusterAnchorDistance;
      if (r != null) return r;
      const o = Zl(i);
      if (!o || o === "")
        return n.d3LinkDistance;
      const a = o.length * 10;
      return Math.max(n.d3LinkDistance, a);
    }), n.d3LinkStrength && t.strength(n.d3LinkStrength);
  }
  static initSimulationForceCharge(t, n) {
    t.theta(n.d3ManyBodyTheta).strength((i) => {
      const r = i, o = n.d3ManyBodyStrength, a = r.expanded ? r.getCircleRadiusCollapsed() : r.getCircleRadius(), s = 10 + Math.sqrt(Math.max(0, a - 10));
      let c = r.weight ?? 1;
      return c *= r.isParent ? 10 : 1, o * (s * s) / 100 * c;
    });
  }
  static initSimulationForceCollide(t, n) {
    const i = n.d3CollideRadiusMultiplier;
    t.radius((r) => {
      const o = r;
      return o.expanded ? i * o.getCircleRadius() + 20 : o.getCircleRadius() ? i * o.getCircleRadius() : n.d3CollideRadius;
    }).strength(n.d3CollideStrength);
  }
  static initSimulationForceClusterRadialConstraint(t, n) {
    t.strength(n.d3CollideStrength);
  }
  update() {
    this.layout ? (this.layout.update(), Object.assign(this.options.layout, this.layout.getSpacing())) : this.scheduleTune();
    const t = this.graph.getMutableNodes().filter((i) => i.visible);
    this.simulation.nodes(t);
    const n = this.simulation.force("link");
    n && n.id((i) => i.id).links(this.getActiveEdges()), this.restart();
  }
  /** @private */
  getActiveEdges() {
    const t = new Set(
      this.graph.getMutableNodes().filter((a) => a.visible).map((a) => a.id)
    ), n = (a) => {
      let s = a;
      for (; s && !t.has(s.id); ) s = s.parentNode;
      return s;
    }, i = (a, s) => a < s ? `${a}|${s}` : `${s}|${a}`, r = [], o = /* @__PURE__ */ new Set();
    for (const a of this.graph.getMutableEdges()) {
      if (!a.visibleIgnoringLayer) continue;
      const s = a.source, c = a.target;
      if (!s.isChild && !c.isChild) {
        r.push(a), o.add(i(s.id, c.id));
        continue;
      }
      if (s.isChild && c.isChild) continue;
      const l = s.isChild ? c : s, f = n(s.isChild ? s : c);
      if (!f || f.id === l.id) continue;
      const v = i(l.id, f.id);
      o.has(v) || (o.add(v), r.push(this.clusterAnchorLink(l, f)));
    }
    return r;
  }
  /**
   * A force-only link tying an external node to an expanded cluster it connects
   * into. Not a real Edge — never rendered, never registered on the nodes — just
   * the `{source, target, distance}` the link force needs. Its distance is the
   * cluster radius (plus the base link distance) so the node rests outside the bubble.
   * @private
   */
  clusterAnchorLink(t, n) {
    return {
      id: `cluster-anchor-${t.id}-${n.id}`,
      source: t,
      target: n,
      __clusterAnchorDistance: n.getCircleRadius() + this.options.d3LinkDistance
    };
  }
  enable() {
    this.avgTickDuration = 0, this.options.enabled = !0, this.start(!1);
  }
  disable() {
    this.options.enabled = !1, this.stop();
  }
  /**
   * Pause the simulation
   */
  pause() {
    this.engineRunning = !1, this.slowTickThresholdReached = !1;
  }
  /**
   * Restart the simulation with rendering on each animation frame.
   */
  restart() {
    this.startSimulationTime = (/* @__PURE__ */ new Date()).getTime(), this.runTickCount = 0, this.engineRunning = !0, this.slowTickThresholdReached = !1;
  }
  /**
   * Start the simulation with rendering on each animation frame.
   */
  async start(t = !0) {
    if (t && (this.tuneNow({ reheat: !1 }), await this.runSimulationWorkerRouter()), !this.options.enabled) {
      this.engineRunning = !1;
      return;
    }
    this.engineRunning = !0, this.slowTickThresholdReached = !1, this.callbacks.onStart && this.callbacks.onStart(this), this.animationFrameId === null && this.startAnimationLoop();
  }
  /**
   * Manually stop the simulation and cancel animation frame.
   */
  stop() {
    this.engineRunning = !1, this.autoTuneTimer !== null && (clearTimeout(this.autoTuneTimer), this.autoTuneTimer = null), this.animationFrameId !== null && (cancelAnimationFrame(this.animationFrameId), this.animationFrameId = null), this.simulation.stop(), this.callbacks.onStop && this.callbacks.onStop(this);
  }
  /**
   * Start the simulation loop with rendering on each animation frame.
   */
  startAnimationLoop() {
    const t = () => {
      this.animationFrameId = requestAnimationFrame(t), this.simulationTick();
    };
    this.engineRunning = !0, this.simulation.alpha(0.01).restart(), this.animationFrameId = requestAnimationFrame(t);
  }
  /**
   * Evaluate at each tick to update the simulation state and request rendering
   */
  simulationTick() {
    if (this.engineRunning) {
      !this.dragInProgress && this.cooledDown() && (this.engineRunning = !1, this.simulation.stop(), this.callbacks.onStop && this.callbacks.onStop(this)), this.totalTickCount++, this.runTickCount++;
      const t = performance.now();
      this.simulation.tick(), this.graph.nextTick(), this.updateTickMetrics(performance.now() - t), this.callbacks.onTick && this.callbacks.onTick(this), this.graphInteraction.simulationTick(), this.totalTickCount % 10 === 0 && this.graphInteraction.simulationSlowTick();
    }
  }
  /**
   * Is the current run finished? The tick budget is the real wall: `d3AlphaDecay` is
   * per *tick*, so a wall-clock budget would truncate every graph ticking below 60fps —
   * the heavy ones, which need the settling most.
   *
   * The ms budget stays as a backstop, times {@link COOLDOWN_WALL_GRACE}, for a hidden
   * tab where rAF drops to ~1fps and a pure tick budget would run on for minutes.
   */
  cooledDown() {
    const t = this.options.cooldownTime / 1e3 * x.NOMINAL_FPS;
    return this.runTickCount >= t || this.options.d3AlphaMin > 0 && this.simulation.alpha() < this.options.d3AlphaMin ? !0 : (/* @__PURE__ */ new Date()).getTime() - this.startSimulationTime > this.options.cooldownTime * x.COOLDOWN_WALL_GRACE;
  }
  updateTickMetrics(t) {
    var n;
    this.avgTickDuration = this.avgTickDuration * 0.9 + t * 0.1, this.avgTickDuration > this.SLOW_TICK_THRESHOLD && (this.slowTickThresholdReached = !0, this.disable(), this.graph.UIManager.showNotification({
      level: "warning",
      title: "Physics engine running slow",
      message: "The physic has been disabled."
    }), (n = this.graph.UIManager.physicsFlyout) == null || n.syncRunState());
  }
  /**
   * Returns a promise that resolves when the simulation stops naturally.
   * Useful for performing actions (like fitAndCenter) after stabilization.
   */
  async waitForSimulationStop() {
    if (this.engineRunning)
      return new Promise((t) => {
        const n = this.callbacks.onStop;
        this.callbacks.onStop = (i) => {
          n == null || n(i), this.callbacks.onStop = n, t();
        };
      });
  }
  isEnabled() {
    return this.options.enabled;
  }
  // Match computed positions to live nodes by id: the layout is handed a
  // different (and differently ordered) node set than the full node map, so
  // they can't be aligned by array index.
  applyComputedPositions(t) {
    const n = new Map(t.map((i) => [i.id, i]));
    for (const i of this.graph.getMutableNodes()) {
      const r = n.get(i.id);
      r && (i.x = r.x, i.y = r.y, i.fx = typeof r.fx == "number" ? r.fx : void 0, i.fy = typeof r.fy == "number" ? r.fy : void 0);
    }
  }
  async computeGraph(t = {}) {
    const { runSimulation: n } = await Promise.resolve().then(() => Pc), i = this.containerBCR, r = this.graph.getMutableNodes(), o = this.graph.getNodes(), a = this.graph.getEdges(), { callbacks: s, ...c } = this.options;
    Object.assign(c, t);
    const { nodes: l } = n(
      o,
      a,
      c,
      i
    );
    this.applyComputedPositions(l), this.graph.updateData(r, void 0, !1);
  }
  async runSimulationWorkerRouter(t = {}) {
    if (this.options.useWorker)
      try {
        await this.runSimulationWorker(t);
        return;
      } catch (n) {
        this.options.useWorker = !1, console.warn(
          "[Pivotick] Simulation Web Worker unavailable (often a CSP blocking blob workers); falling back to the main thread. Set `simulation.useWorker: false` to silence this.",
          n
        );
      }
    await this.computeGraph(t), this.graph.updateLayoutProgress(100, 0, "done");
  }
  async runSimulationWorker(t = {}) {
    const n = this.containerBCR, i = this.graph.getMutableNodes(), r = this.graph.getNodes().map((f) => f.toSimulationDTO()), o = this.graph.getEdges().map((f) => f.toSimulationDTO()), a = (f, v) => {
      this.graph.updateLayoutProgress(f, v, "simulation");
    }, { callbacks: s, ...c } = this.options;
    Object.assign(c, t);
    const { nodes: l } = await hl(
      r,
      o,
      c,
      n,
      a
    );
    this.graph.updateLayoutProgress(100, 0, "rendering"), this.applyComputedPositions(l), this.graph.updateData(i, void 0, !1), this.graph.updateLayoutProgress(100, 0, "done");
  }
  /**
   * Restart the simulation with a bit of heat
   */
  reheat(t = 0.7) {
    this.restart(), this.simulation.alpha(t).restart();
  }
  /**
   * Re-read the node-dependent force accessors and reheat. d3-force caches per-node
   * radius/strength when a force is initialised, not per tick, so a radius mutated mid-run
   * has no effect until the nodes are re-set. For a custom node that measures itself after
   * the opening layout has cooled. No-op when disabled.
   */
  refreshForcesAndReheat(t = 0.5) {
    if (!this.options.enabled) return;
    this.tuneNow({ reheat: !1 });
    const n = this.graph.getMutableNodes().filter((i) => i.visible);
    this.simulation.nodes(n), this.reheat(t);
  }
  // ─── Physics knobs (Physics flyout) ─────────────────────────────────────────
  // Each setter maps an abstract knob (range in PHYSICS_KNOB_RANGES) onto a d3-force
  // domain, re-initialises that force so d3 re-reads its cached per-node array, then
  // reheats. While physics is disabled the value is stored but not reheated.
  //
  // Also auto's only way of expressing itself: a call that does *not* come from auto
  // switches auto off, so a re-tune cannot overwrite a deliberate choice.
  /** Push-apart strength. Knob 0–100 → d3ManyBodyStrength. */
  setRepulsion(t) {
    const n = x.clamp(t, lt.repulsion);
    this.physicsKnobs.repulsion = n, this.options.d3ManyBodyStrength = x.mapLinear(n, lt.repulsion, x.REPULSION_STRENGTH_RANGE), this.scaledForces.d3ManyBodyStrength = this.options.d3ManyBodyStrength, x.initSimulationForceCharge(this.simulationForces.charge, this.options), this.noteManualKnobEdit(), this.reheatIfEnabled();
  }
  /** Preferred edge length. Knob 40–600 (px) → d3LinkDistance. */
  setLinkDistance(t) {
    const n = x.clamp(t, lt.linkDistance);
    this.physicsKnobs.linkDistance = n, this.options.d3LinkDistance = x.mapLinear(n, lt.linkDistance, x.LINK_DISTANCE_RANGE), x.initSimulationForceLink(this.simulationForces.link, this.options), this.noteManualKnobEdit(), this.reheatIfEnabled();
  }
  /** Node spacing. Knob 4–60 → d3CollideRadiusMultiplier (scales each node's collision radius). */
  setCollisionRadius(t) {
    const n = x.clamp(t, lt.collisionRadius);
    this.physicsKnobs.collisionRadius = n, this.options.d3CollideRadiusMultiplier = x.mapLinear(n, lt.collisionRadius, x.COLLIDE_MULTIPLIER_RANGE), x.initSimulationForceCollide(this.simulationForces.collide, this.options), this.noteManualKnobEdit(), this.reheatIfEnabled();
  }
  /** Motion damping. Knob 0–100 → d3VelocityDecay (÷100). Applied live each tick — no reheat. */
  setFriction(t) {
    const n = x.clamp(t, lt.friction);
    this.physicsKnobs.friction = n, this.options.d3VelocityDecay = x.mapLinear(n, lt.friction, x.FRICTION_DECAY_RANGE), this.simulation.velocityDecay(this.options.d3VelocityDecay), this.noteManualKnobEdit();
  }
  /**
   * Pull toward the canvas centre. Knob 0–100 → d3GravityStrengthConnected, with
   * d3GravityStrength (isolated nodes) following as a fixed multiple. Separate components
   * only repel each other, so this is the only thing keeping them in frame.
   */
  setCentering(t) {
    const n = x.clamp(t, lt.centering);
    this.physicsKnobs.centering = n, this.options.d3GravityStrengthConnected = x.gravityForCentering(n), this.options.d3GravityStrength = x.isolatedGravityFor(this.options.d3GravityStrengthConnected), x.initSimulationForceGravity(this.simulationForces.gravity, this.options, this.containerBCR), this.noteManualKnobEdit(), this.reheatIfEnabled();
  }
  /**
   * How long the layout is given to settle, in seconds. Knob 0.5–8 → d3AlphaDecay
   * *and* cooldownTime together: alpha decay sets how fast the sim cools, cooldown
   * is the wall-clock wall that stops it. Moving either alone does nothing — raise
   * the cooldown and the sim is already cold; slow the decay and the wall truncates it.
   */
  setSettleTime(t) {
    const n = x.clamp(t, lt.settleTime);
    this.physicsKnobs.settleTime = n, this.options.d3AlphaDecay = x.alphaDecayForSettleTime(n, this.options.d3AlphaMin), this.options.cooldownTime = n * 1e3, this.simulation.alphaDecay(this.options.d3AlphaDecay), this.noteManualKnobEdit();
  }
  /**
   * Apply a named preset ({@link PHYSICS_PRESETS}): sets every knob and reheats once, at
   * {@link CLICK_REHEAT_ALPHA} rather than the slider default — a preset describes a whole
   * layout, and reaching it from a settled graph takes a fresh layout's worth of travel.
   */
  applyPhysicsPreset(t) {
    this.disableAutoPhysics(), this.writeKnobs(Lc[t]), this.reheatIfEnabled(x.CLICK_REHEAT_ALPHA);
  }
  /**
   * Write a whole knob bundle onto the options + forces, without reheating.
   * Shared by {@link applyPhysicsPreset} and the auto tuner, which each decide
   * their own reheat: one setter per knob would re-init six forces and reheat
   * six times for what is a single logical change.
   */
  writeKnobs(t) {
    this.physicsKnobs = { ...t }, this.options.d3ManyBodyStrength = x.mapLinear(t.repulsion, lt.repulsion, x.REPULSION_STRENGTH_RANGE), this.scaledForces.d3ManyBodyStrength = this.options.d3ManyBodyStrength, this.options.d3LinkDistance = x.mapLinear(t.linkDistance, lt.linkDistance, x.LINK_DISTANCE_RANGE), this.options.d3CollideRadiusMultiplier = x.mapLinear(t.collisionRadius, lt.collisionRadius, x.COLLIDE_MULTIPLIER_RANGE), this.options.d3VelocityDecay = x.mapLinear(t.friction, lt.friction, x.FRICTION_DECAY_RANGE), this.options.d3GravityStrengthConnected = x.gravityForCentering(t.centering), this.options.d3GravityStrength = x.isolatedGravityFor(this.options.d3GravityStrengthConnected), this.options.d3AlphaDecay = x.alphaDecayForSettleTime(t.settleTime, this.options.d3AlphaMin), this.options.cooldownTime = t.settleTime * 1e3, x.initSimulationForceCharge(this.simulationForces.charge, this.options), x.initSimulationForceLink(this.simulationForces.link, this.options), x.initSimulationForceCollide(this.simulationForces.collide, this.options), x.initSimulationForceGravity(this.simulationForces.gravity, this.options, this.containerBCR), this.simulation.velocityDecay(this.options.d3VelocityDecay), this.simulation.alphaDecay(this.options.d3AlphaDecay);
  }
  /** Current knob values, for seeding the Physics-flyout sliders. */
  getPhysicsKnobs() {
    return { ...this.physicsKnobs };
  }
  /** The active layout type — the Physics flyout greys out physics under non-`force` layouts. */
  getLayoutType() {
    return this.options.layout.type;
  }
  /** The active tree layout's spacing multipliers; `1×` under the force layout. */
  getTreeSpacing() {
    var t;
    return ((t = this.layout) == null ? void 0 : t.getSpacing()) ?? { ...kc };
  }
  /**
   * Re-lay-out the tree at new spacing multipliers ({@link TREE_SPACING_RANGE}). No-op
   * under the force layout, where spacing is the physics knobs' job.
   *
   * A redraw *and* a reheat: the pinned axis moves immediately, so the change shows even
   * with physics paused, while the free axis still settles into its new sibling slots.
   */
  setTreeSpacing(t) {
    if (!this.layout) return;
    const n = {};
    t.levelSpacing !== void 0 && (n.levelSpacing = x.clamp(t.levelSpacing, An)), t.siblingSpacing !== void 0 && (n.siblingSpacing = x.clamp(t.siblingSpacing, An)), this.layout.setSpacing(n), Object.assign(this.options.layout, n, { spacing: "manual" }), this.graph.nextTick(), this.reheatIfEnabled();
  }
  /**
   * Where the active tree hangs from. Under the force layout there is no tree, so this
   * reports the finder a tree would start with and no pin.
   */
  getTreeRoot() {
    var t;
    return ((t = this.layout) == null ? void 0 : t.getRoot()) ?? { algorithm: Oc };
  }
  /**
   * Re-hang the tree from another root: `{ rootId }` pins it to that node, `{ algorithm }`
   * drops the pin and lets the finder choose. No-op under the force layout, which has no
   * hierarchy to root.
   *
   * A pinned root is walked ignoring edge direction, so any node — a leaf included — gives
   * a whole tree rather than a stump beside the old one. See {@link TreeLayout.setRoot}.
   */
  setTreeRoot(t) {
    if (!this.layout) return;
    this.layout.setRoot(t);
    const n = this.layout.getRoot();
    Object.assign(this.options.layout, { rootId: n.rootId, rootIdAlgorithmFinder: n.algorithm }), this.graph.nextTick(), this.reheatIfEnabled();
  }
  /** Is the tree spacing tuning itself? `false` under the force layout. */
  isAutoTreeSpacingEnabled() {
    var t;
    return ((t = this.layout) == null ? void 0 : t.isAutoSpacing()) ?? !1;
  }
  /**
   * Hand the tree spacing back to the tuner, which re-derives both multipliers from the
   * node sizes and tree shape and keeps doing so as the graph changes. The counterpart of
   * {@link setTreeSpacing}. No-op under the force layout, where `Auto` is
   * {@link enableAutoPhysics}.
   */
  enableAutoTreeSpacing() {
    this.layout && (this.layout.enableAutoSpacing(), Object.assign(this.options.layout, { spacing: "auto" }), this.graph.nextTick(), this.reheatIfEnabled());
  }
  reheatIfEnabled(t = 0.5) {
    this.suppressReheat || this.options.enabled && this.reheat(t);
  }
  static clamp(t, [n, i]) {
    return Math.max(n, Math.min(i, t));
  }
  static mapLinear(t, n, i) {
    const r = (t - n[0]) / (n[1] - n[0]);
    return i[0] + r * (i[1] - i[0]);
  }
  /** Recover the abstract knob values from a set of d3-force options (inverse of the setters). */
  static knobsFromOptions(t) {
    const n = (r, o, a) => Math.round(x.clamp(x.mapLinear(r, o, lt[a]), lt[a])), i = x.settleTimeFromAlphaDecay(t.d3AlphaDecay, t.d3AlphaMin);
    return {
      repulsion: n(t.d3ManyBodyStrength, x.REPULSION_STRENGTH_RANGE, "repulsion"),
      linkDistance: n(t.d3LinkDistance, x.LINK_DISTANCE_RANGE, "linkDistance"),
      collisionRadius: n(t.d3CollideRadiusMultiplier, x.COLLIDE_MULTIPLIER_RANGE, "collisionRadius"),
      friction: n(t.d3VelocityDecay, x.FRICTION_DECAY_RANGE, "friction"),
      centering: Math.round(x.clamp(
        x.centeringFromGravity(t.d3GravityStrengthConnected),
        lt.centering
      )),
      settleTime: Math.round(x.clamp(i, lt.settleTime) * 10) / 10
    };
  }
  /** `centering` knob → connected-node gravity strength. Quadratic; see CENTERING_STRENGTH_MAX. */
  static gravityForCentering(t) {
    const n = t / lt.centering[1];
    return x.CENTERING_STRENGTH_MAX * n * n;
  }
  static centeringFromGravity(t) {
    const n = Math.sqrt(Math.max(0, t) / x.CENTERING_STRENGTH_MAX);
    return lt.centering[1] * n;
  }
  /**
   * Isolated (degree-0) nodes have no links holding them, only charge pushing them
   * away, so they need a much firmer pull than connected ones — and they need *some*
   * pull even at `centering: 0`, or they leave the canvas entirely.
   */
  static isolatedGravityFor(t) {
    const [n, i] = x.CENTERING_ISOLATED_RANGE;
    return Math.max(n, Math.min(i, t * x.CENTERING_ISOLATED_MULTIPLE));
  }
  /**
   * `settleTime` (s) → the per-tick alpha decay that lands alpha on `alphaMin` after roughly
   * `t · 60` ticks.
   */
  static alphaDecayForSettleTime(t, n) {
    const i = Math.max(1, t * x.NOMINAL_FPS), r = Math.min(0.999, Math.max(1e-6, n));
    return 1 - Math.pow(r, 1 / i);
  }
  static settleTimeFromAlphaDecay(t, n) {
    const i = Math.min(0.999, Math.max(1e-6, n)), r = Math.min(0.999, Math.max(1e-6, t));
    return Math.log(i) / Math.log(1 - r) / x.NOMINAL_FPS;
  }
  // ─── Auto physics ───────────────────────────────────────────────────────────
  /**
   * Whether a graph gets the `Auto` preset. `simulation.physics` forces it either way;
   * otherwise auto is on unless the consumer configured something auto drives, so existing
   * tuning is never quietly taken over. `physics: 'auto'` alongside explicit d3 options is
   * legal — they seed the opening frame and auto takes it from there.
   */
  static shouldAutoTune(t) {
    return t.physics === "auto" ? !0 : t.physics === "manual" ? !1 : !x.AUTO_OWNED_OPTIONS.some((n) => t[n] !== void 0);
  }
  /** Is the `Auto` preset currently driving the knobs? */
  isAutoPhysicsEnabled() {
    return this.autoEnabled;
  }
  /**
   * Turn `Auto` on and tune immediately. `force` because this is the Auto *button*: an
   * answer inside the deadband would otherwise make the click do nothing visible.
   */
  enableAutoPhysics() {
    this.autoEnabled = !0, this.tuneNow({ alpha: x.CLICK_REHEAT_ALPHA, force: !0 });
  }
  /** Turn `Auto` off, leaving the knobs wherever they currently sit. */
  disableAutoPhysics() {
    this.autoEnabled = !1, this.autoTuneTimer !== null && (clearTimeout(this.autoTuneTimer), this.autoTuneTimer = null);
  }
  /** The last tuning pass — what auto saw and what it decided. @private */
  getAutoRun() {
    return this.autoLastRun;
  }
  /** A user (or a consumer) turning a knob themselves takes auto out of the loop. */
  noteManualKnobEdit() {
    this.applyingAutoKnobs || this.disableAutoPhysics();
  }
  /** Collapse the triggers that arrive together — a pivot fires one per node. */
  scheduleTune() {
    this.autoEnabled && (this.autoTuneTimer !== null && clearTimeout(this.autoTuneTimer), this.autoTuneTimer = setTimeout(() => {
      this.autoTuneTimer = null, this.tuneNow();
    }, x.AUTO_DEBOUNCE_MS));
  }
  /**
   * Run the active strategy and apply what it decided. `reheat: false` is for callers
   * about to reheat anyway, so one logical change stays one reheat; `alpha` and `force`
   * are for the Auto button — see {@link enableAutoPhysics}.
   */
  tuneNow(t = {}) {
    var l;
    const { reheat: n = !0, alpha: i = x.AUTO_REHEAT_ALPHA, force: r = !1 } = t;
    if (!this.autoEnabled || this.options.layout.type !== "force") return;
    const o = this.buildAutoContext();
    if (o.nodeCount === 0) return;
    const a = Cc(o), c = Object.keys(a).every((f) => {
      const [v, d] = lt[f];
      return Math.abs(a[f] - this.physicsKnobs[f]) <= (d - v) * x.AUTO_DEADBAND;
    }) && !r;
    if (this.autoLastRun = { context: o, knobs: c ? this.getPhysicsKnobs() : a, skipped: c }, !c) {
      this.applyingAutoKnobs = !0, this.suppressReheat = !0;
      try {
        this.writeKnobs(a);
      } finally {
        this.suppressReheat = !1, this.applyingAutoKnobs = !1;
      }
      n && this.reheatIfEnabled(i), (l = this.graph.UIManager.physicsFlyout) == null || l.syncAutoKnobs(this.getPhysicsKnobs());
    }
  }
  /**
   * What auto is allowed to see: the container at zoom 1, the nodes the sim holds and their
   * radii. The zoom transform is never read — the *zoomed* viewport would loop against
   * `fitAndCenter` (zoom out → more apparent space → spread → re-fit). The container rather
   * than the canvas, for the reason {@link containerBCR} gives.
   */
  buildAutoContext() {
    const t = this.containerBCR, n = this.graph.getMutableNodes().filter((c) => c.visible), i = this.getActiveEdges();
    let r = 0, o = 0, a = 0;
    for (const c of n) {
      const l = c.expanded ? c.getCircleRadiusCollapsed() : c.getCircleRadius();
      r += l, o = Math.max(o, l), a += Math.PI * l * l;
    }
    const s = Dc(
      n.map((c) => c.id),
      i.map((c) => [c.source.id, c.target.id])
    );
    return {
      canvas: { width: t.width, height: t.height },
      nodeCount: n.length,
      radii: { mean: n.length ? r / n.length : 0, max: o, totalArea: a },
      edgeCount: i.length,
      componentCount: s.count,
      looseNodeFraction: s.looseNodeFraction,
      current: this.getPhysicsKnobs()
    };
  }
  /**
   * @private
   */
  createDragBehavior() {
    return Ca().filter(() => !this.graph.editing.connectManager.isActiveAndNotIdle()).on("start.draggedelement", (t, n) => {
      this.graphInteraction.hasActiveMultiselection() ? this.dragSelection = this.graphInteraction.getSelectedNodes().map((i) => {
        const { node: r } = i;
        return r.freeze(), {
          node: r,
          dx: r.x - n.x,
          dy: r.y - n.y
        };
      }) : (this.dragSelection = [], n.freeze());
    }).on("drag.draggedelement", (t, n) => {
      if (!this.dragInProgress && this.isEnabled() && (this.dragInProgress = !0, this.restart(), this.simulation.alphaTarget(0.3).restart()), this.graphInteraction.hasActiveMultiselection())
        this.dragSelection.forEach(({ node: i, dx: r, dy: o }) => {
          const a = this.applySnap(t.x + r), s = this.applySnap(t.y + o);
          i.fx = a, i.fy = s, i.x = a, i.y = s;
        });
      else {
        const i = this.applySnap(t.x), r = this.applySnap(t.y);
        n.fx = i, n.fy = r, n.x = i, n.y = r;
      }
      if (this.graphInteraction.dragging(t.sourceEvent, t.subject), !this.engineRunning || !this.isEnabled()) {
        const i = this.graphInteraction.hasActiveMultiselection() ? this.dragSelection.map((r) => r.node) : [n];
        this.graph.nextTickFor(i);
      }
    }).on("end.draggedelement", (t, n) => {
      !t.active && this.dragInProgress && (this.dragInProgress = !1, this.restart(), this.simulation.alphaTarget(this.options.d3AlphaTarget).restart()), this.options.freezeNodesOnDrag || (this.graphInteraction.hasActiveMultiselection() ? (this.dragSelection.forEach(({ node: i }) => i.unfreeze()), this.dragSelection = []) : n.unfreeze()), this.graphInteraction.dragended(t.sourceEvent, t.subject);
    });
  }
  isDragging() {
    return this.dragInProgress;
  }
  toggleGridSnapping() {
    this.options.gridSnappingEnabled = !this.options.gridSnappingEnabled;
  }
  toggleFreezeNodesOnDrag() {
    this.options.freezeNodesOnDrag = !this.options.freezeNodesOnDrag;
  }
  isFreezeNodesOnDrag() {
    return this.options.freezeNodesOnDrag;
  }
  isGridSnappingEnabled() {
    return this.options.gridSnappingEnabled;
  }
  toggleFitViewOnExpandCollapse() {
    this.options.fitViewOnExpandCollapse = !this.options.fitViewOnExpandCollapse;
  }
  isFitViewOnExpandCollapse() {
    return this.options.fitViewOnExpandCollapse;
  }
  applySnap(t) {
    return this.options.gridSnappingEnabled ? Math.round(t / this.options.gridSize) * this.options.gridSize : t;
  }
  /**
   * Snap a graph-space coordinate to the grid when grid-snapping is enabled
   * (a no-op otherwise). Public so non-simulation draggables (e.g. notes) can
   * snap on the same grid as nodes.
   */
  snapToGrid(t) {
    return this.applySnap(t);
  }
  getForceSimulation() {
    return this.simulationForces;
  }
  getSimulation() {
    return this.simulation;
  }
  /**
   * Allows to change the layout of the graph
   * 
   * @example
   * ```ts
   * changeLayout('tree', {
   *     layout: {
   *          horizontal: false,
   *          rootIdAlgorithmFinder: 'FirstZeroInDegree'
   *     }
   * })
   * ```
   */
  async changeLayout(t, n = {}) {
    var i;
    this.layout && ((i = this.layout) == null || i.unregisterLayout(), this.layout = void 0), n = n ?? {}, n.layout = n.layout ?? {}, n.layout.type = t, t === "force" ? (x.initSimulationForceCharge(this.simulationForces.charge, this.options), x.initSimulationForceCollide(this.simulationForces.collide, this.options)) : t === "tree" && (this.layout = new $(this.graph, this.simulation, this.simulationForces, n.layout)), this.options.layout.type = t, this.update(), this.pause(), await this.runSimulationWorkerRouter(n), this.restart(), await this.waitForSimulationStop(), this.graph.renderer.fitAndCenterWhenSettled();
  }
};
// d3-force domains each knob maps onto; the knob's own range is in PHYSICS_KNOB_RANGES.
E(x, "REPULSION_STRENGTH_RANGE", [0, -400]), // repulsion 0..100 (more negative = stronger)
E(x, "LINK_DISTANCE_RANGE", [40, 600]), // linkDistance 40..600 (identity, px)
E(x, "COLLIDE_MULTIPLIER_RANGE", [0.6, 2.4]), // collisionRadius 4..60
E(x, "FRICTION_DECAY_RANGE", [0, 1]), // friction 0..100 → velocityDecay
// `centering` is the odd one out: measured against real layouts, gravity does
// nothing below ~0.005 and crushes the graph above ~0.2, so a linear knob would
// spend most of its travel on values that make no difference. The map is
// quadratic instead, so the useful band sits mid-slider.
E(x, "CENTERING_STRENGTH_MAX", 0.2), /** Isolated nodes get a fixed multiple of the connected strength… */
E(x, "CENTERING_ISOLATED_MULTIPLE", 4), /** …clamped, so they never fly off at `centering: 0` nor snap to a point at 100. */
E(x, "CENTERING_ISOLATED_RANGE", [0.1, 0.3]), /** Simulation options auto derives; setting any of them opts a graph out of auto. */
E(x, "AUTO_OWNED_OPTIONS", [
  "d3LinkDistance",
  "d3ManyBodyStrength",
  "d3CollideRadiusMultiplier",
  "d3VelocityDecay",
  "d3GravityStrength",
  "d3GravityStrengthConnected",
  "d3AlphaDecay",
  "cooldownTime"
]), /** Triggers inside this window collapse into a single tune. */
E(x, "AUTO_DEBOUNCE_MS", 150), /** A knob has to move by this fraction of its range before auto bothers applying it. */
E(x, "AUTO_DEADBAND", 0.04), /** Auto relaxes the layout from where it is; it never restarts it. */
E(x, "AUTO_REHEAT_ALPHA", 0.3), /**
 * Heat for an *explicit* preset or `Auto` click. A click means "lay this graph
 * out like that", so it gets what a fresh layout gets — a slider drag keeps the
 * gentler {@link reheatIfEnabled} default, and auto's own background re-tune
 * keeps {@link AUTO_REHEAT_ALPHA}.
 */
E(x, "CLICK_REHEAT_ALPHA", 1), /** Ticks per second the alpha schedule is written against (rAF at full speed). */
E(x, "NOMINAL_FPS", 60), /**
 * How far past `cooldownTime` the wall-clock backstop lets a run go. Only ever
 * binding on a throttled tab; see {@link cooledDown}.
 */
E(x, "COOLDOWN_WALL_GRACE", 4), /**
 * Stand-in size used until the container has a real one. A graph can be built while
 * hidden, where `getBoundingClientRect()` reads 0×0 — and a zero area would have the
 * auto tuner fit the layout into no space at all, collapsing every node onto the
 * gravity point.
 */
E(x, "FALLBACK_CONTAINER_SIZE", { width: 1e3, height: 800 });
let En = x;
const kr = 1e4, xn = 2e4, Nn = 0.15 * xn;
self.onmessage = (e) => {
  var b, g, w;
  if (e.data.source !== "simulation-worker-wrapper") return;
  const { nodes: t, edges: n, options: i, canvasBCR: r } = e.data, o = t.map((_) => {
    const R = new Nr(_.id, _.data, _.style);
    return R.setCircleRadius(_._circleRadius ?? 10), typeof _.x == "number" && (R.x = _.x), typeof _.y == "number" && (R.y = _.y), typeof _.fx == "number" && (R.fx = _.fx), typeof _.fy == "number" && (R.fy = _.fy), R;
  }), a = new Map(o.map((_) => [_.id, _])), { simulation: s, simulationForces: c } = En.initSimulationForces(i, r), l = [];
  for (const _ of n) {
    const R = a.get(_.from.id), y = a.get(_.to.id);
    if (R && y) {
      const C = _.style ?? {};
      l.push(new In(_.id, R, y, _.data, C, _.directed));
    }
  }
  s.nodes(o);
  const f = s.force("link");
  f && f.id((_) => _.id).links(l), ((b = i.layout) == null ? void 0 : b.type) === "tree" ? $.registerForcesOnSimulation(
    o,
    l,
    s,
    c,
    i.layout,
    r,
    $
  ) : ((g = i.layout) == null ? void 0 : g.type) === "egoTree" && $.registerForcesOnSimulation(
    o,
    l,
    s,
    c,
    i.layout,
    r,
    ue
  );
  let v = i.warmupTicks || xn;
  v = v === "auto" ? xn : v, v = v - Nn;
  let d = 0.3;
  s.alphaTarget(d);
  const m = (/* @__PURE__ */ new Date()).getTime();
  let S;
  for (let _ = 0; _ < v && !((/* @__PURE__ */ new Date()).getTime() - m > kr || (/* @__PURE__ */ new Date()).getTime() - m > i.cooldownTime || Rn(i, s, d) && (/* @__PURE__ */ new Date()).getTime() - m > i.cooldownTime * 0.15); ++_)
    _ % 5 === 0 && (S = sr(_, (/* @__PURE__ */ new Date()).getTime() - m, i), postMessage({ type: "tick", progress: S, elapsedTime: (/* @__PURE__ */ new Date()).getTime() - m })), s.tick();
  d = 0, s.alphaTarget(d), s.alpha(1);
  for (let _ = 0; _ < Nn && !(Rn(i, s, d) && (/* @__PURE__ */ new Date()).getTime() - m > i.cooldownTime * 0.15); ++_)
    s.tick(), _ % 5 === 0 && (S = sr(v + _, (/* @__PURE__ */ new Date()).getTime() - m, i), postMessage({ type: "tick", progress: S, elapsedTime: (/* @__PURE__ */ new Date()).getTime() - m }));
  postMessage({ type: "tick", progress: 1, elapsedTime: (/* @__PURE__ */ new Date()).getTime() - m }), ((w = i.layout) == null ? void 0 : w.type) === "tree" && $.simulationDone(
    o,
    l,
    s,
    i.layout
  ), postMessage({
    type: "done",
    nodes: o.map((_) => _.toDict()),
    edges: l.map((_) => _.toDict())
  });
};
function Fc(e, t, n, i) {
  var m, S, b;
  const r = e.map((g) => {
    const w = new Nr(g.id, g.getData(), g.getStyle());
    return w.weight = g.weight || 1, w.setCircleRadius(g.getCircleRadius()), typeof g.x == "number" && (w.x = g.x), typeof g.y == "number" && (w.y = g.y), typeof g.fx == "number" && (w.fx = g.fx), typeof g.fy == "number" && (w.fy = g.fy), w;
  }), o = new Map(r.map((g) => [g.id, g])), { simulation: a, simulationForces: s } = En.initSimulationForces(n, i), c = [];
  for (const g of t) {
    const w = o.get(g.from.id), _ = o.get(g.to.id);
    if (w && _) {
      const R = g.getStyle() ?? {};
      c.push(new In(g.id, w, _, g.getData(), R, g.directed));
    }
  }
  a.nodes(r);
  const l = a.force("link");
  l && l.id((g) => g.id).links(c), (((m = n.layout) == null ? void 0 : m.type) === "tree" || ((S = n.layout) == null ? void 0 : S.type) === "egoTree") && $.registerForcesOnSimulation(
    r,
    c,
    a,
    s,
    n.layout,
    i,
    $
  );
  let f;
  n.warmupTicks === "auto" || n.warmupTicks == null ? f = xn : f = n.warmupTicks, f = f - Nn;
  let v = 0.3;
  a.alphaTarget(v);
  const d = (/* @__PURE__ */ new Date()).getTime();
  for (let g = 0; g < f && !((/* @__PURE__ */ new Date()).getTime() - d > kr || (/* @__PURE__ */ new Date()).getTime() - d > n.cooldownTime || Rn(n, a, v) && (/* @__PURE__ */ new Date()).getTime() - d > n.cooldownTime * 0.15); ++g)
    a.tick();
  v = 0, a.alphaTarget(v), a.alpha(1);
  for (let g = 0; g < Nn && !(Rn(n, a, v) && (/* @__PURE__ */ new Date()).getTime() - d > n.cooldownTime * 0.15); ++g)
    a.tick();
  return ((b = n.layout) == null ? void 0 : b.type) === "tree" && $.simulationDone(
    r,
    c,
    a,
    n.layout
  ), {
    nodes: r,
    edges: c
  };
}
function sr(e, t, n) {
  return t / n.cooldownTime;
}
function Rn(e, t, n) {
  return e.d3AlphaMin > 0 && t.alpha() - n < e.d3AlphaMin;
}
const Pc = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  runSimulation: Fc
}, Symbol.toStringTag, { value: "Module" }));
export {
  Fc as runSimulation
};
