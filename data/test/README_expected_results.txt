# Unit Normalizer Quick Test Files

Use these files to test both:
1. standardization of embedded-unit keys like `temp_F=90`
2. normalization into canonical units

## Intended canonical units
- temperature -> degC
- pressure -> Pa
- time -> s
- rotational_speed -> rpm
- force -> N
- gas_flow -> sccm
- liquid_flow -> mL/min
- power -> W
- frequency -> Hz
- length -> m
- thickness -> nm
- mass -> kg
- percentage -> %

## A few expected conversions
- 90 F -> 32.2222 degC
- 300 K -> 26.85 degC
- 1.2 kPa -> 1200 Pa
- 750 mTorr -> ~99.9918 Pa
- 14.7 psi -> ~101352.933 Pa
- 2 min -> 120 s
- 1500 ms -> 1.5 s
- 0.5 h -> 1800 s
- 2 rps -> 120 rpm
- 31.4159265359 rad/s -> 300 rpm
- 10 lbf -> ~44.4822 N
- 1.5 slm -> 1500 sccm
- 2 L/h -> 33.3333 mL/min
- 1.2 kW -> 1200 W
- 850 mW -> 0.85 W
- 2500 A -> 250 nm
- 0.8 um thickness -> 800 nm
- 0.47 fraction -> 47 %