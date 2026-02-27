"""
Tests for FCAI article classification logic.
"""

from __future__ import annotations

from motor_vehicles.scraping.fcai_articles import classify_sales_article


class TestClassifySalesArticle:
    """Tests for keyword-based article title classification."""

    # --- Should match as sales articles ---

    def test_typical_monthly_title(self):
        assert classify_sales_article("New vehicle sales results for January 2025")

    def test_vfacts_title(self):
        assert classify_sales_article("VFACTS: December 2024 new car sales")

    def test_market_update_title(self):
        assert classify_sales_article("Australian automotive sales market update")

    def test_buyer_confidence_title(self):
        assert classify_sales_article("Buyer confidence drives record sales")

    def test_utes_dominate_title(self):
        assert classify_sales_article("Utes dominate as SUV demand grows")

    def test_consumer_demand_title(self):
        assert classify_sales_article("Consumer demand remains strong in Q3")

    def test_record_outlook_title(self):
        assert classify_sales_article("New record but outlook remains tough")

    def test_sales_in_month_title(self):
        assert classify_sales_article("Strong sales in October continue trend")

    def test_solid_vehicle_title(self):
        assert classify_sales_article("Solid vehicle performance across all segments")

    def test_slow_start_title(self):
        assert classify_sales_article("Slow start for new year but recovery expected")

    def test_sales_reach_title(self):
        assert classify_sales_article("Sales reach one million milestone")

    def test_case_insensitive(self):
        assert classify_sales_article("NEW VEHICLE SALES RESULTS FOR MARCH")

    # --- Should NOT match (excluded topics) ---

    def test_motorcycle_excluded(self):
        assert not classify_sales_article("Motorcycle sales hit new high in 2024")

    def test_scooter_excluded(self):
        assert not classify_sales_article("Scooter market grows in urban areas")

    def test_atv_excluded(self):
        assert not classify_sales_article("ATV safety standards update")

    def test_road_safety_excluded(self):
        assert not classify_sales_article("Road safety campaign launches nationally")

    def test_ev_charger_excluded(self):
        assert not classify_sales_article("EV charger rollout across Australia")

    def test_budget_excluded(self):
        assert not classify_sales_article("Federal budget impact on auto industry")

    def test_emissions_excluded(self):
        assert not classify_sales_article("New emissions standards for 2025")

    def test_tyre_stewardship_excluded(self):
        assert not classify_sales_article("Tyre stewardship scheme update")

    # --- Should NOT match (unrelated topics) ---

    def test_generic_industry_news(self):
        assert not classify_sales_article("Industry leaders meet at annual conference")

    def test_ceo_appointment(self):
        assert not classify_sales_article("New CEO appointed to lead FCAI")

    def test_empty_title(self):
        assert not classify_sales_article("")

    # --- Historical title patterns (pre-2022) ---

    def test_car_market_ahead_of_record(self):
        assert classify_sales_article("September Keeps Car Market Ahead of Record")

    def test_motor_industry_record(self):
        assert classify_sales_article("July Record for Motor Industry")

    def test_monthly_sales_record(self):
        assert classify_sales_article("Motor Industry Posts Monthly sales Record")

    def test_record_may_lifts(self):
        assert classify_sales_article("Record May Lifts sales Across All States")

    def test_suv_sales_overtake(self):
        assert classify_sales_article("Suv sales Overtake Passenger Cars for First Time Ever")

    def test_solid_january_start(self):
        assert classify_sales_article("Solid January Start Follows Industry's Record Year")

    def test_car_sales_growth(self):
        assert classify_sales_article("Car sales Growth Fuelled by Suvs and Light Commercials")

    def test_record_year_for_2016(self):
        assert classify_sales_article("Record sales Year for 2016 Flags Dramatic Shift in Buyer Preferences")

    def test_on_target_for_record(self):
        assert classify_sales_article("Market on Target for a Record Year")

    def test_car_industry_smashes(self):
        assert classify_sales_article("Car Industry Smashes All-Time sales Records in June")

    def test_first_quarter_new_vehicle(self):
        assert classify_sales_article("First Quarter New Vehicle sales Result")

    # --- Edge cases: exclude takes precedence ---

    def test_exclude_overrides_include(self):
        """If title matches both sales keyword and exclude keyword, exclude wins."""
        assert not classify_sales_article("Motorcycle vehicle sales results for June")
