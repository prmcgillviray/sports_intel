import math

"""
TACTICAL BRAIN MODULE
---------------------
The mathematical core of THE ORACLE.
Responsibility: Pure function calculations for EV, Kelly Criterion, and Odds Conversion.
Philosophy: Stateless, high-performance, zero I/O.
"""

def american_to_decimal(american_odds):
    """
    Converts American Odds (e.g., +150, -110) to Decimal Odds (e.g., 2.5, 1.909).
    """
    try:
        if american_odds > 0:
            return round(1 + (american_odds / 100), 3)
        else:
            return round(1 + (100 / abs(american_odds)), 3)
    except Exception as e:
        return 1.0  # Fail safe

def calculate_ev(model_prob, decimal_odds):
    """
    Calculates Expected Value (EV).
    Formula: (Probability_Win * (Decimal_Odds - 1)) - (Probability_Loss * 1)
    
    Returns: Float representing percentage EV (e.g., 0.05 is 5% edge).
    """
    if decimal_odds <= 1:
        return -1.0 # Invalid odds
    
    prob_win = model_prob
    prob_loss = 1 - model_prob
    
    # Amount won per unit bet (Odds - 1)
    amount_won = decimal_odds - 1
    
    ev = (prob_win * amount_won) - (prob_loss * 1)
    return round(ev, 4)

def kelly_criterion(model_prob, decimal_odds, bankroll, fraction=0.25):
    """
    Calculates the optimal stake size using Fractional Kelly Criterion.
    
    Args:
        model_prob (float): Internal model probability (0.0 - 1.0)
        decimal_odds (float): Market decimal odds
        bankroll (float): Total current bankroll
        fraction (float): Kelly multiplier (0.25 = Quarter Kelly for risk management)
        
    Returns:
        float: Recommended wager amount
    """
    if decimal_odds <= 1:
        return 0.0

    b = decimal_odds - 1 # Net odds received on the wager
    p = model_prob
    q = 1 - p
    
    # Full Kelly formula: f* = (bp - q) / b
    f_star = ((b * p) - q) / b
    
    # Apply fractional constraint and bankroll
    if f_star > 0:
        stake_percentage = f_star * fraction
        wager = bankroll * stake_percentage
        return round(wager, 2)
    else:
        return 0.0

def assess_edge(model_prob, market_implied_prob):
    """
    Simple check to see if Model sees an event as more likely than Market does.
    """
    return model_prob > market_implied_prob
