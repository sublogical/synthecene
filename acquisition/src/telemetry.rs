use num::{Float, Zero};

pub trait Accumulator<T:Float>  {
    fn add(&mut self, value:T, weight:T, time:T) -> T;

    fn value(&self) -> T;
}
fn exponential_decay<T:Float>(λ:T, n:T, dt:T) -> T {
    n*(λ*dt).exp()
}


/// The purpose of this function is to calculate an exponential decay. The 
/// variable λ is the decay rate, N is the initial value, and dt is the time 
/// elapsed. The function returns the value of N after it has decayed by the 
/// specified amount.
pub struct ExponentialDecayAccumulator<T:Float> {
    value: T,
    weight: T,
    time: T,
    λ:T
}

impl <T:Float> ExponentialDecayAccumulator<T> {
    pub fn init(λ:T, time:T) -> ExponentialDecayAccumulator<T> {
        ExponentialDecayAccumulator {
            value: Zero::zero(),
            weight: Zero::zero(),
            time,
            λ
        }
    }
}

impl <T:Float> Accumulator<T> for ExponentialDecayAccumulator<T> {
    fn add(&mut self, value:T, weight:T, time:T) -> T {
        let dt = time - self.time;
        self.value = exponential_decay(self.λ, self.value, dt) + value;
        self.weight = exponential_decay(self.λ, self.weight, dt) + weight;

        self.value()
    }

    fn value(&self) -> T {
        self.value / self.weight
    }
}

#[derive(Clone, Debug, Default)]
pub struct MeanAccumulator<T:Float> {
    value: T,
    weight: T,
}


impl <T:Float> Accumulator<T> for MeanAccumulator<T> {
    fn add(&mut self, value:T, weight:T, _time:T) -> T {
        self.value = self.value + value;
        self.weight = self.weight + weight;
        
        self.value()
    }

    fn value(&self) -> T {
        self.value / self.weight
    }
}

#[derive(Clone, Debug, Default)]
pub struct MaxAccumulator<T:Float> {
    value: T
}

impl <T:Float> Accumulator<T> for MaxAccumulator<T> {
    fn add(&mut self, value:T, _weight:T, _time:T) -> T {
        if value > self.value {
            self.value = value;
        }
        self.value
    }

    fn value(&self) -> T {
        self.value
    }
}

#[derive(Clone, Debug, Default)]
pub struct MinAccumulator<T:Float> {
    value: T
}

impl <T:Float> Accumulator<T> for MinAccumulator<T> {
    fn add(&mut self, value:T, _weight:T, _time:T) -> T {
        if value < self.value {
            self.value = value;
        }
        self.value
    }

    fn value(&self) -> T {
        self.value
    }
}

#[derive(Clone, Debug, Default)]
pub struct SumAccumulator<T:Float> {
    value: T
}

impl <T:Float> Accumulator<T> for SumAccumulator<T> {
    fn add(&mut self, value:T, _weight:T, _time:T) -> T {
        if value < self.value {
            self.value = self.value + value;
        }
        self.value
    }

    fn value(&self) -> T {
        self.value
    }
}



pub type Stat=Box<dyn Accumulator<f64> + Send + Sync>;
